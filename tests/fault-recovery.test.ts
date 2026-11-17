/**
 * Fault-injection integration tests.
 *
 * Each scenario runs a real child sync process against a real SQLite file,
 * lets the configured one-shot fault terminate it (exit code 42), restarts a
 * fresh process on the same file, and asserts the final state equals the
 * fault-free reference replay. Recovery must not depend on a new source event.
 *
 * The mid_prepare_big_tx fault is configurable by target transaction and
 * cumulative prepared-row count; its fault-exit log carries the actual
 * cumulative count. Pause/concurrency scenarios live in pause-control.test.ts
 * so the two files run in parallel under `npm test`.
 */

import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import {
  startSource,
  startSync,
  readAllReadings,
  httpJson,
  parseStructuredLogs,
  sleep,
} from './helpers';
import {
  newDir,
  cleanupDirs,
  waitForExit,
  readOnDiskState,
  assertConvergedWithReplay,
  readdir,
} from './fault-utils';
import { TX } from '../src/fixtures/fixtures';
import type { FaultPoint, SyncStatus } from '../src/shared/protocol';

after(cleanupDirs);

const FAULT_CHAIN: Array<{ point: FaultPoint; description: string }> = [
  { point: 'after_receive_persist', description: 'right after a received page is durable' },
  { point: 'mid_prepare_big_tx', description: 'after the 10,000th cumulatively prepared row of the 20k tx' },
  { point: 'before_publish_commit', description: 'immediately before a publish short-transaction' },
  { point: 'after_publish_commit', description: 'immediately after a publish short-transaction' },
];

test('FAULT chain: all four one-shot crash points recover on real process restart', async () => {
  const dir = await newDir('fault-chain');
  const dbFile = path.join(dir, 'sync.db');
  const src = await startSource({ overlap: true });
  const seenFaults: string[] = [];
  try {
    // Every fault process reopens the SAME sqlite file left by the previous
    // crash and resumes from its durable position - so each restart is itself
    // the recovery step, with no new source event required to make progress.
    for (const { point, description } of FAULT_CHAIN) {
      const crashed = await startSync({ sourcePort: src.port, dbFile, fault: point, pageLimit: 1000 });
      const exitCode = await waitForExit(crashed.sync);
      assert.equal(exitCode, 42, `${point}: process must hard-exit (got ${exitCode})`);
      const faultLogs = parseStructuredLogs(crashed.log);
      const exitLine = faultLogs.find((l) => l.event === 'fault-exit' && l.fault === point);
      assert.ok(exitLine, `${point}: structured fault-exit log present (${description})`);
      if (point === 'mid_prepare_big_tx') {
        // Configurable default: the 20k tx's cumulative prepare watermark is
        // exactly 10,000 when the process exits, and prior batches stay durable.
        assert.equal(exitLine.txId, TX.T10.id);
        assert.equal(exitLine.targetTxId, TX.T10.id);
        assert.equal(exitLine.targetRows, 10000);
        assert.equal(exitLine.cumulativePreparedRows, 10000, 'log reports the actual cumulative prepared count');
      }
      seenFaults.push(point);
      assert.ok((await readdir(dir)).includes('sync.db'), `${point}: sqlite file survived the crash`);
    }

    // Final fault-free process on the same file converges to the oracle.
    // All log events were already durable before the mid-prepare crash, so
    // this convergence does not depend on the source sending anything new.
    const finalApp = await startSync({ sourcePort: src.port, dbFile, pageLimit: 1000 });
    try {
      const { status } = await assertConvergedWithReplay(finalApp.port);
      assert.deepEqual(seenFaults, FAULT_CHAIN.map((f) => f.point));
      assert.equal(status.publishedTransactions, 9, 'no double business effect across restarts');
    } finally {
      await finalApp.stop();
    }
  } finally {
    await src.stop();
  }
});

test('FAULT mid_prepare_big_tx: configurable target tx/cumulative rows; staged rows invisible', async () => {
  const dir = await newDir('fault-config');
  const dbFile = path.join(dir, 'sync.db');
  const src = await startSource({ overlap: false });
  try {
    // Custom configuration: exit at 2,500 cumulatively prepared rows of TX-0010
    // (a non-batch-aligned value, proving the cut lands on the exact row).
    const first = await startSync({
      sourcePort: src.port,
      dbFile,
      fault: 'mid_prepare_big_tx',
      midPrepareTx: TX.T10.id,
      midPrepareRows: 2500,
      pageLimit: 1000,
    });
    const exitCode = await waitForExit(first.sync);
    assert.equal(exitCode, 42);
    const exitLine = parseStructuredLogs(first.log).find((l) => l.event === 'fault-exit');
    assert.ok(exitLine);
    assert.equal(exitLine.fault, 'mid_prepare_big_tx');
    assert.equal(exitLine.txId, TX.T10.id);
    assert.equal(exitLine.cumulativePreparedRows, 2500, 'actual cumulative prepared count is logged');
    assert.equal(exitLine.targetRows, 2500);

    const onDisk = readOnDiskState(dbFile);
    assert.equal(onDisk.tx.status, 'open', 'mid-prepare tx stays open, not rolled back');
    assert.equal(onDisk.stagedRows, 2500, 'committed prepare progress is durable on disk');
    assert.equal(onDisk.publishedBigRows, 0, 'staged-but-unpublished rows are invisible');

    // Recovery with fault injection DISABLED and no new source events: the
    // source still serves the same fixture, but the process resumes purely
    // from durable state and converges on its own.
    const second = await startSync({ sourcePort: src.port, dbFile, pageLimit: 1000, prepareDelayMs: 1 });
    try {
      let probes = 0;
      let sawIntermediate = false;
      const deadline = Date.now() + 25000;
      while (Date.now() < deadline) {
        const { items } = await readAllReadings(second.port, 250);
        probes += 1;
        const bigVisible = items.filter((i) => i.recordId.startsWith('BIG-')).length;
        if (bigVisible !== 0 && bigVisible !== 19999) sawIntermediate = true;
        const st = await httpJson<SyncStatus>(`http://127.0.0.1:${second.port}/status`);
        if (st.body.publishedTransactions === 9 && st.body.openTransactions === 1) break;
        await sleep(8);
      }
      assert.ok(probes > 10, `read API stayed responsive during resume (${probes} probes)`);
      assert.equal(sawIntermediate, false, 'no partially-prepared big-tx rows leaked to reads');
      const { items, status } = await assertConvergedWithReplay(second.port);
      assert.equal(items.filter((i) => i.recordId.startsWith('BIG-')).length, 19999);
      assert.equal(status.schemaVersion, 2);
    } finally {
      await second.stop();
    }
  } finally {
    await src.stop();
  }
});
