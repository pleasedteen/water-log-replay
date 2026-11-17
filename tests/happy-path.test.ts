import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import {
  startSource,
  startSync,
  httpJson,
  waitForStatus,
  readAllReadings,
  parseStructuredLogs,
  sleep,
  makeTempHarness,
  type Harness,
} from './helpers';
import { referenceReplay } from './reference-replayer';
import { FIXTURE_BASE_OFFSET, LARGE_TX_ROW_COUNT, TX } from '../src/fixtures/fixtures';
import type { ListResponse, SyncStatus } from '../src/shared/protocol';

let h: Harness;
let sourceStop: () => Promise<void>;
let syncStop: () => Promise<void>;

before(async () => {
  h = await makeTempHarness('happy');
  const src = await startSource({ overlap: true });
  sourceStop = src.stop;
  h.sourcePort = src.port;
  h.sourceLog = src.log;
  const sync = await startSync({ sourcePort: src.port, dbFile: h.dbFile, pageLimit: 1000 });
  syncStop = sync.stop;
  h.syncPort = sync.port;
  h.syncLog = sync.log;
});

after(async () => {
  await syncStop?.();
  await sourceStop?.();
  await h.teardown();
});

test('HAPPY: full replay reaches reference state incl. migration, ordering and 20k tx', async () => {
  // T7 (missing row) stays open forever, so wait for the published count of
  // all other transactions and the last durable offset instead of zero-open.
  const finalStatus = await waitForStatus(
    h.syncPort,
    (s: SyncStatus) =>
      s.publishedTransactions === 9 &&
      s.openTransactions === 1 &&
      s.schemaVersion === 2 &&
      BigInt(s.receivedOffset) > FIXTURE_BASE_OFFSET + 20000n,
    45000,
  );

  const expected = referenceReplay();
  assert.equal(expected.schemaVersion, 2, 'reference expects migrated schema');

  // --- status counters --------------------------------------------------
  assert.equal(finalStatus.publishedTransactions, expected.publishedTxIds.length);
  assert.deepEqual(finalStatus.running, true);
  assert.ok(finalStatus.retryCount >= 0);

  // --- full list matches the independent oracle -------------------------
  const { schemaVersion, items } = await readAllReadings(h.syncPort, 1000);
  assert.equal(schemaVersion, 2);
  assert.equal(items.length, expected.records.size, 'visible record count matches reference');
  const byId = new Map(items.map((i) => [i.recordId, i]));
  for (const [id, want] of expected.records) {
    const got = byId.get(id);
    assert.ok(got, `record ${id} visible`);
    assert.equal(got.stationId, want.stationId);
    assert.equal(got.sampledAt, want.sampledAt);
    assert.equal(got.nitrateMgL, want.nitrateMgL);
    assert.equal(got.methodCode, want.methodCode);
    assert.ok(!('nitrate_mg_l' in got), 'v1 field name must not leak on v2 API');
  }

  // --- large transaction: 19,999 BIG ids + R-0001 override --------------
  const bigRecords = items.filter((i) => i.recordId.startsWith('BIG-'));
  assert.equal(bigRecords.length, LARGE_TX_ROW_COUNT - 1);
  const r1 = byId.get('R-0001');
  assert.equal(r1.methodCode, 'lab-beta'); // last row of T10 overrides earlier T1/T2
  assert.equal(r1.nitrateMgL, ((LARGE_TX_ROW_COUNT - 1) % 100) / 10);

  // --- deleted records are gone; single GET -> 404 ----------------------
  const deleted = await httpJson(`http://127.0.0.1:${h.syncPort}/readings/R-0002`);
  assert.equal(deleted.status, 404);
  const missing = await httpJson(`http://127.0.0.1:${h.syncPort}/readings/R-0006`); // belongs to never-completed T7
  assert.equal(missing.status, 404, 'incomplete transaction never leaks partial rows');

  // --- single GET works and reports schema version ----------------------
  const one = await httpJson<{ schemaVersion: number; item: any }>(
    `http://127.0.0.1:${h.syncPort}/readings/R-0003`,
  );
  assert.equal(one.status, 200);
  assert.equal(one.body.item.nitrateMgL, 3.3); // retest value, not the withdrawn 9.9
  assert.equal(one.body.schemaVersion, 2);

  // --- keyset pagination: no duplicates, strict order -------------------
  const page1 = await httpJson<ListResponse>(`http://127.0.0.1:${h.syncPort}/readings?limit=5`);
  assert.equal(page1.body.items.length, 5);
  assert.ok(page1.body.nextRecordId);
  const page2 = await httpJson<ListResponse>(
    `http://127.0.0.1:${h.syncPort}/readings?limit=5&after=${encodeURIComponent(page1.body.nextRecordId!)}`,
  );
  assert.ok(page2.body.items[0].recordId > page1.body.items[4].recordId);
  assert.ok(!page1.body.items.some((i) => page2.body.items.some((j) => j.recordId === i.recordId)));

  // --- structured logs expose batch sizes and short-commit publishes ----
  const logs = parseStructuredLogs(h.syncLog);
  const prepares = logs.filter((l) => l.event === 'prepare-batch' || l.event === 'tx-prepared');
  assert.ok(prepares.some((l) => l.batchCap === 200), 'prepare batches capped at 200');
  const bigBatches = prepares.filter((l) => l.txId === TX.T10.id);
  assert.ok(bigBatches.length >= 100, `20k tx prepared across many batches (got ${bigBatches.length})`);
  assert.ok(logs.some((l) => l.event === 'tx-published' && l.via === 'short-commit'));
  assert.ok(logs.some((l) => l.event === 'schema-applied' && l.schemaVersion === 2));

  // overlap duplicates did not corrupt counters
  assert.equal(finalStatus.preparedTransactions, 0);
  await sleep(10);
});

test('HAPPY: duplicate / overlapping deliveries are idempotent on restart', async () => {
  // Restart the process against the SAME sqlite file; no new source event needed.
  await syncStop();
  const sync2 = await startSync({ sourcePort: h.sourcePort, dbFile: h.dbFile, pageLimit: 1000 });
  syncStop = sync2.stop;
  h.syncPort = sync2.port;
  h.syncLog = sync2.log;

  const status = await waitForStatus(
    h.syncPort,
    (s: SyncStatus) => s.publishedTransactions === 9 && s.openTransactions === 1,
    20000,
  );
  const { items } = await readAllReadings(h.syncPort);
  const expected = referenceReplay();
  assert.equal(items.length, expected.records.size);
  assert.equal(status.schemaVersion, 2);
  // no double business effect: still exactly one published row per tx
  assert.equal(status.publishedTransactions, 9);
});
