/**
 * Fault-injection integration tests.
 *
 * Each scenario runs a real child sync process against a real SQLite file,
 * lets the configured one-shot fault terminate it (exit code 42), restarts a
 * fresh process on the same file, and asserts the final state equals the
 * fault-free reference replay. Recovery must not depend on a new source event.
 */

import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, readdir } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {
  startSource,
  startSync,
  waitForStatus,
  readAllReadings,
  httpJson,
  parseStructuredLogs,
  sleep,
} from './helpers';
import { referenceReplay } from './reference-replayer';
import type { FaultPoint, SyncStatus } from '../src/shared/protocol';

const tempDirs: string[] = [];
async function newDir(label: string): Promise<string> {
  const d = await mkdtemp(path.join(os.tmpdir(), `wq-${label}-`));
  tempDirs.push(d);
  return d;
}
after(async () => {
  for (const d of tempDirs) await rm(d, { recursive: true, force: true });
});

async function waitForExit(child: import('node:child_process').ChildProcess, timeoutMs = 30000): Promise<number | null> {
  if (child.exitCode !== null) return child.exitCode;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('process did not exit on fault')), timeoutMs);
    child.once('exit', (code) => {
      clearTimeout(timer);
      resolve(code);
    });
  });
}

const FAULT_CHAIN: Array<{ point: FaultPoint; description: string }> = [
  { point: 'after_receive_persist', description: 'right after a received page is durable' },
  { point: 'mid_prepare_big_tx', description: 'after the first 200-row prepare batch of the 20k tx' },
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
    // The process boundaries are: receive-persist -> mid-prepare ->
    // before-publish -> after-publish -> clean convergence.
    for (const { point, description } of FAULT_CHAIN) {
      const crashed = await startSync({ sourcePort: src.port, dbFile, fault: point, pageLimit: 1000 });
      const exitCode = await waitForExit(crashed.sync);
      assert.equal(exitCode, 42, `${point}: process must hard-exit (got ${exitCode})`);
      const faultLogs = parseStructuredLogs(crashed.log);
      assert.ok(
        faultLogs.some((l) => l.event === 'fault-exit' && l.fault === point),
        `${point}: structured fault-exit log present (${description})`,
      );
      seenFaults.push(point);
      assert.ok((await readdir(dir)).includes('sync.db'), `${point}: sqlite file survived the crash`);
    }

    // Final fault-free process on the same file converges to the oracle.
    // All log events were already durable before the mid-prepare crash, so
    // this convergence does not depend on the source sending anything new.
    const finalApp = await startSync({ sourcePort: src.port, dbFile, pageLimit: 1000 });
    try {
      const status = await waitForStatus(
        finalApp.port,
        (s: SyncStatus) => s.publishedTransactions === 9 && s.openTransactions === 1 && s.schemaVersion === 2,
        30000,
      );
      const { items, schemaVersion } = await readAllReadings(finalApp.port);
      const expected = referenceReplay();
      assert.deepEqual(seenFaults, FAULT_CHAIN.map((f) => f.point));
      assert.equal(schemaVersion, expected.schemaVersion);
      assert.equal(items.length, expected.records.size);
      const got = new Map(items.map((i) => [i.recordId, i]));
      for (const [id, want] of expected.records) {
        const g = got.get(id);
        assert.ok(g, `record ${id} present after full crash chain`);
        assert.equal(g.nitrateMgL, want.nitrateMgL);
        assert.equal(g.methodCode, want.methodCode);
      }
      const r6 = await httpJson(`http://127.0.0.1:${finalApp.port}/readings/R-0006`);
      assert.equal(r6.status, 404, 'never-completed T7 stays invisible after every crash');
      assert.equal(status.publishedTransactions, 9, 'no double business effect across restarts');
    } finally {
      await finalApp.stop();
    }
  } finally {
    await src.stop();
  }
});

test('FAULT mid_prepare_big_tx: staged-but-unpublished rows are invisible while preparing', async () => {
  const dir = await newDir('visibility');
  const dbFile = path.join(dir, 'sync.db');
  const src = await startSource({ overlap: false });
  try {
    const first = await startSync({ sourcePort: src.port, dbFile, fault: 'mid_prepare_big_tx', pageLimit: 1000 });
    await waitForExit(first.sync);

    // Restart and immediately hammer the read API while the 20k tx is being
    // prepared in 200-row batches: no BIG-* record may ever appear pre-publish.
    // A small per-batch delay keeps the prepare window open long enough for
    // many concurrent read probes while every batch stays capped at 200 rows.
    const second = await startSync({ sourcePort: src.port, dbFile, pageLimit: 1000, prepareDelayMs: 8 });
    try {
      let sawIntermediate = false;
      let probes = 0;
      const deadline = Date.now() + 20000;
      while (Date.now() < deadline) {
        const { body: status } = await httpJson<SyncStatus>(`http://127.0.0.1:${second.port}/status`);
        const { items } = await readAllReadings(second.port, 250);
        probes += 1;
        const bigVisible = items.filter((i) => i.recordId.startsWith('BIG-')).length;
        // Publish is atomic per transaction: either zero BIG rows (still
        // staging) or all 19,999 (published). Anything in between is a leak.
        if (bigVisible !== 0 && bigVisible !== 19999) sawIntermediate = true;
        if (status.publishedTransactions === 9 && status.openTransactions === 1) break;
        await sleep(15);
      }
      assert.ok(probes > 10, `read API stayed responsive during prepare (${probes} probes)`);
      assert.equal(sawIntermediate, false, 'no partially-prepared big-tx rows leaked to reads');

      const finalStatus = await waitForStatus(
        second.port,
        (s: SyncStatus) => s.publishedTransactions === 9 && s.openTransactions === 1,
        20000,
      );
      const { items } = await readAllReadings(second.port);
      assert.equal(items.filter((i) => i.recordId.startsWith('BIG-')).length, 19999);
      assert.equal(finalStatus.schemaVersion, 2);
    } finally {
      await second.stop();
    }
  } finally {
    await src.stop();
  }
});

test('PAUSE: mid-flight pause stops after current batch; resume then restart completes', async () => {
  const dir = await newDir('pause');
  const dbFile = path.join(dir, 'sync.db');
  const src = await startSource({ overlap: true });
  try {
    // Start paused (no autostart), then start manually to verify /sync/start.
    const app = await startSync({ sourcePort: src.port, dbFile, autostart: false, pageLimit: 1000 });
    try {
      const start1 = await httpJson<{ started: boolean }>(`http://127.0.0.1:${app.port}/sync/start`, { method: 'POST' });
      assert.equal(start1.body.started, true);
      const start2 = await httpJson<{ started: boolean }>(`http://127.0.0.1:${app.port}/sync/start`, { method: 'POST' });
      assert.equal(start2.body.started, false, 'duplicate start does not create a second loop');

      // Pause almost immediately and confirm it settles.
      await sleep(20);
      const paused = await httpJson(`http://127.0.0.1:${app.port}/sync/pause`, { method: 'POST' });
      assert.equal(paused.status, 200);
      const st1 = await httpJson<SyncStatus>(`http://127.0.0.1:${app.port}/status`);
      assert.equal(st1.body.running, false);
      const offsetAtPause = st1.body.receivedOffset;

      // While paused: reads still work and the offset does not advance.
      await sleep(80);
      const st2 = await httpJson<SyncStatus>(`http://127.0.0.1:${app.port}/status`);
      assert.equal(st2.body.receivedOffset, offsetAtPause, 'no pulling while paused');
      await httpJson(`http://127.0.0.1:${app.port}/readings?limit=5`);

      // Resume, then pause again to prove repeatability.
      await httpJson(`http://127.0.0.1:${app.port}/sync/start`, { method: 'POST' });
      await sleep(20);
      await httpJson(`http://127.0.0.1:${app.port}/sync/pause`, { method: 'POST' });
      await httpJson(`http://127.0.0.1:${app.port}/sync/start`, { method: 'POST' });

      await waitForStatus(
        app.port,
        (s: SyncStatus) => s.publishedTransactions === 9 && s.openTransactions === 1,
        30000,
      );
    } finally {
      await app.stop();
    }

    // Fresh process restart from the durable position alone.
    const restarted = await startSync({ sourcePort: src.port, dbFile, pageLimit: 1000 });
    try {
      const status = await waitForStatus(
        restarted.port,
        (s: SyncStatus) => s.publishedTransactions === 9 && s.openTransactions === 1,
        20000,
      );
      const { items } = await readAllReadings(restarted.port);
      assert.equal(items.length, referenceReplay().records.size);
      assert.equal(status.schemaVersion, 2);
    } finally {
      await restarted.stop();
    }
  } finally {
    await src.stop();
  }
});

test('RETRY: HTTP 503 triggers backoff retry and pause interrupts the wait', async () => {
  const dir = await newDir('retry');
  const dbFile = path.join(dir, 'sync.db');
  // Every 6th distinct page request fails once with 503 (3 failures total).
  const src = await startSource({ failEvery: 6 });
  try {
    const app = await startSync({ sourcePort: src.port, dbFile, autostart: false, pageLimit: 1000 });
    try {
      await httpJson(`http://127.0.0.1:${app.port}/sync/start`, { method: 'POST' });
      // Wait until at least one retry has been counted.
      await waitForStatus(app.port, (s: SyncStatus) => s.retryCount > 0, 15000);
      const paused = await httpJson(`http://127.0.0.1:${app.port}/sync/pause`, { method: 'POST' });
      assert.equal(paused.status, 200, 'pause returns even while backing off');
      const st = await httpJson<SyncStatus>(`http://127.0.0.1:${app.port}/status`);
      assert.equal(st.body.running, false);
      assert.ok(st.body.retryCount >= 1);
      assert.ok(st.body.lastError !== null || st.body.retryCount >= 1);
      const logs = parseStructuredLogs(app.log);
      assert.ok(logs.some((l) => l.event === 'fetch-retry'), 'structured retry diagnostics present');

      // Resume and converge anyway.
      await httpJson(`http://127.0.0.1:${app.port}/sync/start`, { method: 'POST' });
      await waitForStatus(
        app.port,
        (s: SyncStatus) => s.publishedTransactions === 9 && s.openTransactions === 1,
        45000,
      );
      const { items } = await readAllReadings(app.port);
      assert.equal(items.length, referenceReplay().records.size);
    } finally {
      await app.stop();
    }
  } finally {
    await src.stop();
  }
});
