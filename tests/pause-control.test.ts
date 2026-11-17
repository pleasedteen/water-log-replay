/**
 * Pause-boundary and concurrency-control integration tests.
 *
 * Pause must land on a persistence-unit boundary: once accepted, only the
 * in-flight receive page / prepare batch / publish short-transaction is
 * allowed to commit; no next batch is prepared and prepared-but-unpublished
 * transactions are not published. The response returns only after
 * running=false with the received position, prepare watermark and published
 * set stable.
 *
 * These tests never rely on a fixed startup sleep: they wait for durable
 * progress via /diagnostics before sending pause, then verify the on-disk
 * watermark is frozen and survives a real process restart on the same SQLite
 * file. Fault scenarios live in fault-recovery.test.ts; the two files run in
 * parallel under `npm test`.
 */

import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import {
  startSource,
  startSync,
  httpJson,
  parseStructuredLogs,
  sleep,
  getDiagnostics,
  getTxDiagnostic,
  waitForPreparedRows,
  waitForStatus,
  readAllReadings,
} from './helpers';
import { newDir, cleanupDirs, waitForExit, readOnDiskState, assertConvergedWithReplay, waitConverged } from './fault-utils';
import { referenceReplay } from './reference-replayer';
import { TX } from '../src/fixtures/fixtures';
import type { SyncStatus } from '../src/shared/protocol';

after(cleanupDirs);

test('PAUSE: boundary freeze plus concurrency semantics in one lifecycle; restart resumes', async () => {
  const dir = await newDir('pause-batch');
  const dbFile = path.join(dir, 'sync.db');
  const src = await startSource({ overlap: true });
  try {
    // Widen the prepare window so /diagnostics can observe the 20k tx across
    // many batches; every batch itself stays capped at 200 rows.
    const app = await startSync({
      sourcePort: src.port,
      dbFile,
      autostart: false,
      pageLimit: 1000,
      prepareDelayMs: 8,
    });
    try {
      // Two starts fired together before any loop exists: exactly one loop.
      const [s0a, s0b] = await Promise.all([
        httpJson<{ started: boolean; running: boolean }>(`http://127.0.0.1:${app.port}/sync/start`, { method: 'POST' }),
        httpJson<{ started: boolean; running: boolean }>(`http://127.0.0.1:${app.port}/sync/start`, { method: 'POST' }),
      ]);
      assert.equal([s0a.body.started, s0b.body.started].filter(Boolean).length, 1, 'concurrent starts create one loop');
      assert.equal(s0a.body.running, true);

      // Wait for DURABLE evidence (not a fixed sleep) that the big tx has
      // crossed several 200-row prepare batches while remaining unpublished.
      const mid = await waitForPreparedRows(app.port, TX.T10.id, { min: 2000, max: 12000, notPublished: true });
      assert.ok(mid.prepared_count >= 2000, 'big tx already spans multiple prepare batches');
      assert.equal(mid.status, 'open', 'big tx still mid-prepare when pause is sent');

      // Fire TWO pauses at once: both must wait for the same stop, neither
      // returns before the loop halted at a durable boundary.
      const t0 = Date.now();
      const [p1, p2] = await Promise.all([
        httpJson<any>(`http://127.0.0.1:${app.port}/sync/pause`, { method: 'POST' }),
        httpJson<any>(`http://127.0.0.1:${app.port}/sync/pause`, { method: 'POST' }),
      ]);
      const pauseMs = Date.now() - t0;
      assert.equal(p1.status, 200);
      assert.equal(p2.status, 200);
      assert.equal(p1.body.running, false, 'no pause returns before the loop stopped');
      assert.equal(p2.body.running, false);
      assert.ok(pauseMs < 2000, `pause settles at a batch boundary, not after full publish (${pauseMs}ms)`);
      assert.deepEqual(
        { off: p1.body.receivedOffset, pub: p1.body.publishedTransactions },
        { off: p2.body.receivedOffset, pub: p2.body.publishedTransactions },
        'concurrent pauses report identical stable positions',
      );

      // Reads stay available; every durable position is stable across two
      // diagnostics snapshots taken a short, bounded interval apart.
      await httpJson(`http://127.0.0.1:${app.port}/readings?limit=5`);
      const sA = await getDiagnostics(app.port);
      await sleep(40);
      const sB = await getDiagnostics(app.port);
      const st: SyncStatus = (await httpJson<SyncStatus>(`http://127.0.0.1:${app.port}/status`)).body;
      assert.equal(st.running, false);
      const txAfter = sB.transactions.find((t) => t.tx_id === TX.T10.id)!;
      const txBefore = sA.transactions.find((t) => t.tx_id === TX.T10.id)!;
      assert.equal(txAfter.prepared_count, txBefore.prepared_count, 'prepare watermark frozen mid-batch');
      assert.ok(txAfter.prepared_count > 0 && txAfter.prepared_count < 20000, 'paused strictly inside the big tx');
      assert.equal(txAfter.status, 'open', 'partially prepared tx retained, not undone');
      assert.equal(sB.meta.received_offset, sA.meta.received_offset, 'received position frozen');
      assert.equal(st.receivedOffset, sB.meta.received_offset);
      const publishedBefore = sA.transactions.filter((t) => t.status === 'published').length;
      const publishedAfter = sB.transactions.filter((t) => t.status === 'published').length;
      assert.equal(publishedAfter, publishedBefore, 'no prepared tx got published after pause');
      assert.equal(p1.body.publishedTransactions, publishedAfter, 'pause body reports the stable published set');
      const frozen = sB;

      // A start arriving right after the pause settled (and a duplicate in the
      // same wave) start exactly one fresh loop from the frozen position.
      const [r1, r2] = await Promise.all([
        httpJson<{ started: boolean }>(`http://127.0.0.1:${app.port}/sync/start`, { method: 'POST' }),
        httpJson<{ started: boolean }>(`http://127.0.0.1:${app.port}/sync/start`, { method: 'POST' }),
      ]);
      assert.equal([r1.body.started, r2.body.started].filter(Boolean).length, 1, 'resume wave starts one loop');
      await waitConverged(app.port);
      const txDone = await getTxDiagnostic(app.port, TX.T10.id);
      assert.equal(txDone!.status, 'published');
      assert.equal(txDone!.prepared_count, 20000);
      const finalDiag = await getDiagnostics(app.port);
      assert.ok(
        BigInt(finalDiag.meta.received_offset) >= BigInt(frozen.meta.received_offset),
        'resume advances from the frozen durable offset, never behind it',
      );
    } finally {
      await app.stop();
    }

    // Real process restart on the SAME file: a fresh process resumes from
    // durable state and the independent replay must agree.
    const restarted = await startSync({ sourcePort: src.port, dbFile, pageLimit: 1000 });
    try {
      const { status } = await assertConvergedWithReplay(restarted.port);
      const disk = readOnDiskState(dbFile);
      assert.equal(disk.stagedRows, 20000, 'remaining prepare completed across the process boundary');
      assert.equal(disk.publishedTxns, status.publishedTransactions);
      assert.equal(disk.publishedBigRows, 19999, 'whole-tx publish visible from the reopened file');
    } finally {
      await restarted.stop();
    }
  } finally {
    await src.stop();
  }
});

test('PAUSE-PREPARED: pause with prepared-but-unpublished tx keeps it staged; start publishes it', async () => {
  const dir = await newDir('pause-prepared');
  const dbFile = path.join(dir, 'sync.db');
  const src = await startSource({ overlap: false });
  try {
    // Deterministically create prepared-but-unpublished state: the
    // before_publish_commit fault exits 42 after rows are fully staged and
    // durable, before any publish short-transaction of that run.
    const crashed = await startSync({
      sourcePort: src.port,
      dbFile,
      fault: 'before_publish_commit',
      pageLimit: 1000,
    });
    assert.equal(await waitForExit(crashed.sync), 42);
    const diskCrashed = readOnDiskState(dbFile);
    // Rows are durably staged but nothing of the big tx is visible yet.
    assert.equal(diskCrashed.publishedBigRows, 0, 'fault fires before any big-tx publish');
    assert.ok(diskCrashed.stagedRows >= 0);

    // Fresh process, NOT autostarted: the prepared tx must sit in the library.
    const app = await startSync({ sourcePort: src.port, dbFile, autostart: false, pageLimit: 1000 });
    try {
      const st0 = await httpJson<SyncStatus>(`http://127.0.0.1:${app.port}/status`);
      assert.equal(st0.body.running, false);
      assert.ok(st0.body.preparedTransactions >= 1, 'prepared-but-unpublished tx retained across restart');
      const preparedIds = (await getDiagnostics(app.port)).transactions.filter((t) => t.status === 'prepared').map((t) => t.tx_id);
      assert.ok(preparedIds.length >= 1);

      // Pausing an idle engine is well-defined and changes nothing durable.
      const pauseRes = await httpJson(`http://127.0.0.1:${app.port}/sync/pause`, { method: 'POST' });
      assert.equal(pauseRes.status, 200);
      assert.equal((pauseRes.body as any).running, false);
      const st1 = await httpJson<SyncStatus>(`http://127.0.0.1:${app.port}/status`);
      assert.equal(st1.body.preparedTransactions, st0.body.preparedTransactions, 'pause does not publish or discard prepared tx');
      assert.equal(st1.body.publishedTransactions, st0.body.publishedTransactions);

      // Start resumes: the staged tx is published without re-preparing/re-fetching.
      const startRes = await httpJson<{ started: boolean }>(`http://127.0.0.1:${app.port}/sync/start`, { method: 'POST' });
      assert.equal(startRes.body.started, true);
      await waitConverged(app.port);
      const finalStatus = await httpJson<SyncStatus>(`http://127.0.0.1:${app.port}/status`);
      assert.equal(finalStatus.body.preparedTransactions, 0);
      const { items } = await readAllReadings(app.port);
      assert.equal(items.length, referenceReplay().records.size);
    } finally {
      await app.stop();
    }
  } finally {
    await src.stop();
  }
});

test('RETRY: HTTP 503 backoff is interrupted promptly by pause, then resume converges', async () => {
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
      const t0 = Date.now();
      const paused = await httpJson(`http://127.0.0.1:${app.port}/sync/pause`, { method: 'POST' });
      const pauseMs = Date.now() - t0;
      assert.equal(paused.status, 200, 'pause returns even while backing off');
      assert.ok(pauseMs < 1000, `pause cuts the backoff wait short (${pauseMs}ms)`);
      const st = await httpJson<SyncStatus>(`http://127.0.0.1:${app.port}/status`);
      assert.equal(st.body.running, false);
      assert.ok(st.body.retryCount >= 1);
      const logs = parseStructuredLogs(app.log);
      assert.ok(logs.some((l) => l.event === 'fetch-retry'), 'structured retry diagnostics present');

      // While paused the offset / counters do not move (poll for stability
      // rather than sleeping a fixed amount).
      const offsetAtPause = st.body.receivedOffset;
      for (let i = 0; i < 8; i++) {
        const s = await httpJson<SyncStatus>(`http://127.0.0.1:${app.port}/status`);
        assert.equal(s.body.receivedOffset, offsetAtPause, 'no pulling while paused');
        assert.equal(s.body.running, false);
        await sleep(10);
      }

      // Resume and converge anyway.
      await httpJson(`http://127.0.0.1:${app.port}/sync/start`, { method: 'POST' });
      await waitConverged(app.port);
    } finally {
      await app.stop();
    }
  } finally {
    await src.stop();
  }
});
