/**
 * Store-level tests against a real SQLite file (no server, no network):
 * physical v1->v2 migration, late schema events, v1 backlog after migration,
 * out-of-order transaction arrival and bounded prepare batches.
 */

import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SyncStore, MAX_PREPARE_ROWS } from '../src/sync/store';
import type { LogEvent, RowEvent, CommitEvent, SchemaEvent } from '../src/shared/protocol';

let dir: string;
before(async () => {
  dir = await mkdtemp(path.join(os.tmpdir(), 'wq-store-'));
});
after(async () => {
  await rm(dir, { recursive: true, force: true });
});

function row(ev: Partial<RowEvent> & { txId: string; txVersion: string; index: number; recordId: string }): RowEvent {
  return {
    kind: 'row',
    schemaVersion: 1,
    op: 'upsert',
    ...ev,
  } as RowEvent;
}
const commit = (txId: string, txVersion: string, eventCount: number): CommitEvent => ({
  kind: 'commit',
  txId,
  txVersion,
  eventCount,
});
const schemaV2: SchemaEvent = {
  kind: 'schema',
  schemaVersion: 2,
  changes: [
    { type: 'renameColumn', table: 'readings', from: 'nitrate_mg_l', to: 'nitrateMgL' },
    { type: 'addColumn', table: 'readings', column: 'methodCode', ddlType: 'TEXT', nullable: false, defaultForExisting: 'legacy' },
  ],
};

test('MIGRATION: v1 API shape before schema event, physical columns change afterwards', async () => {
  const db = path.join(dir, 'm1.db');
  const store = new SyncStore(db);

  const v1Tx = 'TX-A';
  const events: Array<{ offset: string; event: LogEvent }> = [
    {
      offset: '100',
      event: row({
        txId: v1Tx,
        txVersion: '500',
        index: 0,
        recordId: 'R-A',
        after: { recordId: 'R-A', stationId: 'S1', sampledAt: '2026-01-01T00:00:00Z', nitrate_mg_l: 2.5 },
      }),
    },
    { offset: '101', event: commit(v1Tx, '500', 1) },
  ];
  store.persistPage(events, '101');
  store.applyDueSchemaEvents();
  let r = store.prepareBatch();
  assert.equal(r.finished, true);
  store.publishReady();

  // Pre-migration read shape uses the v1 field name.
  const before = store.getReading('R-A')!;
  assert.equal(before.nitrate_mg_l, 2.5);
  assert.equal('nitrateMgL' in before, false);

  // Schema event arrives late; physical table structure must change.
  const ddlBefore = store.db.prepare("select sql from sqlite_master where name='record_versions'").get() as { sql: string };
  assert.match(ddlBefore.sql, /nitrate_mg_l/);
  store.persistPage([{ offset: '200', event: schemaV2 }], '200');
  const applied = store.applyDueSchemaEvents();
  assert.equal(applied.length, 1);
  const ddlAfter = store.db.prepare("select sql from sqlite_master where name='record_versions'").get() as { sql: string };
  assert.match(ddlAfter.sql, /nitrateMgL/);
  assert.match(ddlAfter.sql, /method_code/);
  assert.doesNotMatch(ddlAfter.sql, /nitrate_mg_l/);

  // Already-published v1 row now reads in v2 shape with legacy method.
  const after = store.getReading('R-A')!;
  assert.equal(after.nitrateMgL, 2.5);
  assert.equal(after.methodCode, 'legacy');
  assert.equal('nitrate_mg_l' in after, false);
  store.close();
});

test('MIGRATION: v1 backlog rows arriving after the schema event become legacy v2 rows', async () => {
  const db = path.join(dir, 'm2.db');
  const store = new SyncStore(db);
  store.persistPage([{ offset: '1', event: schemaV2 }], '1');
  store.applyDueSchemaEvents();
  assert.equal(store.getSchemaVersion(), 2);

  // Old-format transaction delivered out of the log afterwards.
  const events = [
    {
      offset: '2',
      event: row({
        txId: 'TX-OLD',
        txVersion: '100',
        index: 0,
        recordId: 'R-OLD',
        schemaVersion: 1,
        after: { recordId: 'R-OLD', stationId: 'S9', sampledAt: '2026-02-02T00:00:00Z', nitrate_mg_l: 0 },
      }),
    },
    { offset: '3', event: commit('TX-OLD', '100', 1) },
  ];
  store.persistPage(events, '3');
  store.prepareBatch();
  store.publishReady();
  const got = store.getReading('R-OLD')!;
  assert.equal(got.nitrateMgL, 0, 'zero concentration is preserved, not treated as null');
  assert.equal(got.methodCode, 'legacy');

  // A real v2 row keeps its own methodCode.
  const v2Events = [
    {
      offset: '4',
      event: row({
        txId: 'TX-NEW',
        txVersion: '200',
        index: 0,
        recordId: 'R-NEW',
        schemaVersion: 2,
        after: { recordId: 'R-NEW', stationId: 'S9', sampledAt: '2026-02-03T00:00:00Z', nitrateMgL: null, methodCode: 'probe-x' },
      }),
    },
    { offset: '5', event: commit('TX-NEW', '200', 1) },
  ];
  store.persistPage(v2Events, '5');
  store.prepareBatch();
  store.publishReady();
  const neu = store.getReading('R-NEW')!;
  assert.equal(neu.nitrateMgL, null);
  assert.equal(neu.methodCode, 'probe-x');
  store.close();
});

test('ORDERING: newer txVersion wins even when the older transaction arrives later', async () => {
  const db = path.join(dir, 'order.db');
  const store = new SyncStore(db);

  // Newer tx v200 arrives and publishes first.
  store.persistPage(
    [
      {
        offset: '10',
        event: row({ txId: 'TX-NEW', txVersion: '200', index: 0, recordId: 'R-K', after: { recordId: 'R-K', stationId: 'S', sampledAt: 't', nitrate_mg_l: 1.1 } }),
      },
      { offset: '11', event: commit('TX-NEW', '200', 1) },
    ],
    '11',
  );
  store.prepareBatch();
  store.publishReady();
  assert.equal(store.getReading('R-K')!.nitrate_mg_l, 1.1);

  // Older tx v100 shows up late; it must NOT overwrite the newer state.
  store.persistPage(
    [
      {
        offset: '12',
        event: row({ txId: 'TX-OLD', txVersion: '100', index: 0, recordId: 'R-K', after: { recordId: 'R-K', stationId: 'S', sampledAt: 't', nitrate_mg_l: 9.9 } }),
      },
      { offset: '13', event: commit('TX-OLD', '100', 1) },
    ],
    '13',
  );
  store.prepareBatch();
  store.publishReady();
  assert.equal(store.getReading('R-K')!.nitrate_mg_l, 1.1, 'late old tx cannot revert newer state');

  // An even newer delete removes the record; single read -> null (404 at API).
  store.persistPage(
    [
      { offset: '14', event: row({ txId: 'TX-DEL', txVersion: '300', index: 0, recordId: 'R-K', op: 'delete' }) },
      { offset: '15', event: commit('TX-DEL', '300', 1) },
    ],
    '15',
  );
  store.prepareBatch();
  store.publishReady();
  assert.equal(store.getReading('R-K'), null);
  store.close();
});

test('BATCHING: prepare never stages more than 200 rows per batch and resumes across restarts', async () => {
  const db = path.join(dir, 'batch.db');
  const store = new SyncStore(db);
  const N = 1000;
  const entries: Array<{ offset: string; event: LogEvent }> = [];
  for (let i = 0; i < N; i++) {
    entries.push({
      offset: String(1000 + i),
      event: row({
        txId: 'TX-BIG',
        txVersion: '900',
        index: i,
        recordId: `K-${i}`,
        after: { recordId: `K-${i}`, stationId: 'S', sampledAt: 't', nitrate_mg_l: i },
      }),
    });
  }
  entries.push({ offset: String(1000 + N), event: commit('TX-BIG', '900', N) });
  // Persist in chunks to mimic page-by-page streaming without holding all rows twice.
  for (let i = 0; i < entries.length; i += 137) {
    const chunk = entries.slice(i, i + 137);
    store.persistPage(chunk, chunk[chunk.length - 1].offset);
  }

  let batches = 0;
  for (;;) {
    const r = store.prepareBatch();
    if (!r.txId) break;
    assert.ok(r.rows <= MAX_PREPARE_ROWS, `batch size ${r.rows} <= ${MAX_PREPARE_ROWS}`);
    batches += 1;
    if (r.finished) break;
  }
  assert.equal(batches, Math.ceil(N / MAX_PREPARE_ROWS));

  // Nothing visible before publish despite fully staged rows.
  assert.equal(store.listReadings(10, null).items.length, 0);

  // Simulate a restart: a brand-new store over the same file resumes state.
  store.close();
  const reopened = new SyncStore(db);
  const status = reopened.getStatus(false);
  assert.equal(status.preparedTransactions, 1);
  assert.equal(status.publishedTransactions, 0);
  reopened.publishReady();
  assert.equal(reopened.listReadings(N + 10, null).items.length, N);
  reopened.close();
});

test('DURABILITY: offset only advances inside the same transaction as the events', async () => {
  const db = path.join(dir, 'dur.db');
  const store = new SyncStore(db);
  assert.equal(store.getReceivedOffset(), '0');
  store.persistPage(
    [
      {
        offset: '7',
        event: row({ txId: 'TX-X', txVersion: '1', index: 0, recordId: 'R-X', after: { recordId: 'R-X', stationId: 's', sampledAt: 't', nitrate_mg_l: 1 } }),
      },
    ],
    '7',
  );
  assert.equal(store.getReceivedOffset(), '7');
  // Re-deliver the identical event at the same offset: deduped, no double counting.
  store.persistPage(
    [
      {
        offset: '7',
        event: row({ txId: 'TX-X', txVersion: '1', index: 0, recordId: 'R-X', after: { recordId: 'R-X', stationId: 's', sampledAt: 't', nitrate_mg_l: 1 } }),
      },
    ],
    '7',
  );
  const diag = store.diagnostics() as any;
  const x = diag.transactions.find((t: any) => t.tx_id === 'TX-X');
  assert.equal(x.received_count, 1);
  store.close();
});
