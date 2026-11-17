/**
 * Deterministic, network-free fixture set for the source simulator.
 *
 * The module produces a flat, already-shuffled arrival order of log events.
 * Offsets are assigned later by the simulator's in-memory log, starting near
 * Number.MAX_SAFE_INTEGER to prove that offsets/txVersions are treated as
 * decimal strings and bigints rather than JS numbers.
 *
 * Coverage (required by the lab spec):
 *  T1  ordinary insert + update
 *  T2  newer transaction that physically arrives BEFORE the older one (out-of-order)
 *  T3  withdraw-then-retest of the same sample number inside one transaction
 *  T4  delete of a record inside an interleaved (cut-in-half) transaction
 *  T5  repeated operations on one recordId inside a single transaction
 *  T6  commit marker arrives before its row events
 *  T7  a transaction permanently missing one row (never becomes publishable)
 *  T8  v2 rows arrive before the schema event; commit also early
 *  T9  v1 backlog rows arriving after the v2 schema event
 *  T10 a 20,000-row retest transaction, interrupted by other traffic mid-stream
 *  plus deliberate duplicate row/commit events at different offsets
 */

import type { CommitEvent, LogEvent, RowEvent, SchemaEvent, RowMirror } from '../shared/protocol';

export const FIXTURE_BASE_OFFSET = 9007199254740990n;
export const FIXTURE_BASE_TXVERSION = 7000000000000000n;

/** Fixed transaction ids and their source-side commit order (txVersion). */
export const TX = {
  T1: { id: 'TX-0001', version: FIXTURE_BASE_TXVERSION + 1n, eventCount: 2 },
  T2: { id: 'TX-0002', version: FIXTURE_BASE_TXVERSION + 2n, eventCount: 1 },
  T3: { id: 'TX-0003', version: FIXTURE_BASE_TXVERSION + 3n, eventCount: 3 },
  T4: { id: 'TX-0004', version: FIXTURE_BASE_TXVERSION + 4n, eventCount: 2 },
  T5: { id: 'TX-0005', version: FIXTURE_BASE_TXVERSION + 5n, eventCount: 3 },
  T6: { id: 'TX-0006', version: FIXTURE_BASE_TXVERSION + 6n, eventCount: 1 },
  T7: { id: 'TX-0007', version: FIXTURE_BASE_TXVERSION + 7n, eventCount: 2 }, // only 1 row ever sent
  T8: { id: 'TX-0008', version: FIXTURE_BASE_TXVERSION + 8n, eventCount: 2 },
  T9: { id: 'TX-0009', version: FIXTURE_BASE_TXVERSION + 9n, eventCount: 1 },
  T10: { id: 'TX-0010', version: FIXTURE_BASE_TXVERSION + 10n, eventCount: 20000 },
} as const;

export const LARGE_TX_ROW_COUNT = 20000;
export const LARGE_TX_PREPARE_BATCH = 200;

let rowSeq = 0;
function row(
  tx: { id: string; version: bigint },
  index: number,
  schemaVersion: 1 | 2,
  op: 'upsert' | 'delete',
  recordId: string,
  after?: RowMirror,
): RowEvent {
  rowSeq += 1;
  const ev: RowEvent = {
    kind: 'row',
    txId: tx.id,
    txVersion: tx.version.toString(),
    index,
    schemaVersion,
    op,
    recordId,
  };
  if (after !== undefined) ev.after = after;
  return ev;
}

function commit(tx: { id: string; version: bigint }, eventCount: number): CommitEvent {
  return { kind: 'commit', txId: tx.id, txVersion: tx.version.toString(), eventCount };
}

const schemaV2Event: SchemaEvent = {
  kind: 'schema',
  schemaVersion: 2,
  changes: [
    { type: 'renameColumn', table: 'readings', from: 'nitrate_mg_l', to: 'nitrateMgL' },
    { type: 'addColumn', table: 'readings', column: 'methodCode', ddlType: 'TEXT', nullable: false, defaultForExisting: 'legacy' },
  ],
};

function v1(recordId: string, stationId: string, sampledAt: string, nitrate: number | null) {
  return { recordId, stationId, sampledAt, nitrate_mg_l: nitrate };
}
function v2(recordId: string, stationId: string, sampledAt: string, nitrate: number | null, methodCode: string) {
  return { recordId, stationId, sampledAt, nitrateMgL: nitrate, methodCode };
}

/** The 20,000 row events of T10 (generated lazily via an array builder). */
export function buildLargeTxRows(): RowEvent[] {
  const out: RowEvent[] = [];
  for (let i = 0; i < LARGE_TX_ROW_COUNT; i++) {
    // Last row overrides R1 so that, once published, R1's final state comes
    // from this big transaction (and the prepare must not leak intermediate states).
    const recordId = i === LARGE_TX_ROW_COUNT - 1 ? 'R-0001' : `BIG-${String(i).padStart(5, '0')}`;
    out.push(
      row(
        TX.T10,
        i,
        2,
        'upsert',
        recordId,
        v2(recordId, 'ST-BIG', '2026-10-01T08:00:00Z', (i % 100) / 10, i % 3 === 0 ? 'lab-alpha' : 'lab-beta'),
      ),
    );
  }
  return out;
}

export interface FixtureData {
  /** Arrival order, before offset assignment. */
  events: LogEvent[];
  /** human-readable description of each required scenario. */
  scenarios: Record<string, string>;
  largeTx: { txId: string; rowCount: number };
  /** ids of records whose final published state tests should assert. */
  expectedFinalRecordIds: string[];
}

export function buildFixture(): FixtureData {
  const events: LogEvent[] = [];

  // ---- T2 (newer) arrives entirely before T1 (older) --------------------
  events.push(row(TX.T2, 0, 1, 'upsert', 'R-0001', v1('R-0001', 'ST-01', '2026-09-01T10:00:00Z', 4.2)));
  events.push(commit(TX.T2, TX.T2.eventCount));

  events.push(row(TX.T1, 0, 1, 'upsert', 'R-0001', v1('R-0001', 'ST-01', '2026-09-01T09:00:00Z', 5.5)));
  events.push(row(TX.T1, 1, 1, 'upsert', 'R-0002', v1('R-0002', 'ST-02', '2026-09-01T09:05:00Z', 0))); // 0 is a valid concentration

  // ---- T3: withdraw then retest same sample number inside one tx --------
  events.push(row(TX.T3, 0, 1, 'upsert', 'R-0003', v1('R-0003', 'ST-03', '2026-09-02T08:00:00Z', 9.9)));
  events.push(row(TX.T3, 1, 1, 'delete', 'R-0003'));
  events.push(row(TX.T3, 2, 1, 'upsert', 'R-0003', v1('R-0003', 'ST-03', '2026-09-02T11:30:00Z', 3.3)));
  events.push(commit(TX.T3, TX.T3.eventCount));

  // ---- T4 cut in half by T5 traffic (interleaved transactions) ----------
  events.push(row(TX.T4, 0, 1, 'upsert', 'R-0002', v1('R-0002', 'ST-02', '2026-09-03T08:00:00Z', 1.1)));

  // ---- T5: same recordId operated on three times in one tx --------------
  events.push(row(TX.T5, 0, 1, 'upsert', 'R-0004', v1('R-0004', 'ST-04', '2026-09-03T09:00:00Z', 2.0)));
  events.push(row(TX.T5, 1, 1, 'upsert', 'R-0004', v1('R-0004', 'ST-04', '2026-09-03T09:10:00Z', null))); // null: no valid concentration
  events.push(row(TX.T5, 2, 1, 'upsert', 'R-0004', v1('R-0004', 'ST-04', '2026-09-03T09:20:00Z', 2.8)));
  events.push(commit(TX.T5, TX.T5.eventCount));

  events.push(row(TX.T4, 1, 1, 'delete', 'R-0002')); // second half of T4: R-0002 finally deleted by a newer tx than T1
  events.push(commit(TX.T4, TX.T4.eventCount));

  // ---- T6: commit marker first, rows afterwards --------------------------
  events.push(commit(TX.T6, TX.T6.eventCount));
  events.push(row(TX.T6, 0, 1, 'upsert', 'R-0005', v1('R-0005', 'ST-05', '2026-09-04T07:00:00Z', 6.6)));

  // ---- T7: only one of two rows is ever sent; commit present ------------
  events.push(row(TX.T7, 0, 1, 'upsert', 'R-0006', v1('R-0006', 'ST-06', '2026-09-04T08:00:00Z', 7.7)));
  events.push(commit(TX.T7, TX.T7.eventCount)); // stays open forever (waiting index 1)

  // ---- T8: v2 rows + commit BEFORE the schema event ----------------------
  events.push(row(TX.T8, 0, 2, 'upsert', 'R-0007', v2('R-0007', 'ST-07', '2026-09-05T08:00:00Z', 1.4, 'ion-chroma')));
  events.push(row(TX.T8, 1, 2, 'upsert', 'R-0008', v2('R-0008', 'ST-07', '2026-09-05T08:05:00Z', null, 'ion-chroma')));
  events.push(commit(TX.T8, TX.T8.eventCount));

  // ---- T10 begins: first 500 rows, then other traffic cuts in ------------
  const large = buildLargeTxRows();
  events.push(...large.slice(0, 500));

  // ---- T9: v1 backlog rows arriving after v2 traffic but before schema ---
  events.push(row(TX.T9, 0, 1, 'upsert', 'R-0009', v1('R-0009', 'ST-09', '2026-09-06T08:00:00Z', 8.8)));

  // ---- schema v2 declaration finally arrives -----------------------------
  events.push(schemaV2Event);
  events.push(commit(TX.T9, TX.T9.eventCount));

  // ---- rest of the big transaction ---------------------------------------
  events.push(...large.slice(500));
  events.push(commit(TX.T10, TX.T10.eventCount));

  // ---- late T1 commit (its rows were already durable) --------------------
  events.push(commit(TX.T1, TX.T1.eventCount));

  // ---- deliberate duplicates at new offsets (re-delivered events) --------
  events.push(row(TX.T1, 0, 1, 'upsert', 'R-0001', v1('R-0001', 'ST-01', '2026-09-01T09:00:00Z', 5.5)));
  events.push(commit(TX.T1, TX.T1.eventCount));
  events.push(commit(TX.T8, TX.T8.eventCount));
  events.push(large[0]); // duplicate of the first big-tx row

  return {
    events,
    largeTx: { txId: TX.T10.id, rowCount: LARGE_TX_ROW_COUNT },
    expectedFinalRecordIds: ['R-0001', 'R-0003', 'R-0004', 'R-0005', 'R-0007', 'R-0008', 'R-0009', 'BIG-19998'],
    scenarios: {
      ordinary: 'T1 ordinary inserts; T2 newer value delivered first; final R-0001 must be T10 override anyway',
      withdrawRetest: 'T3 delete then re-upsert of R-0003 inside one atomic transaction',
      outOfOrder: 'T2 (txVersion 2) physically arrives before T1 (txVersion 1)',
      repeatedSameRecord: 'T5 touches R-0004 three times; only index 2 is visible after publish',
      commitEarly: 'T6 commit marker precedes its row event',
      schemaInterleaving: 'T8 v2 rows/commit arrive before schema event; T9 v1 backlog arrives around it',
      missingRow: 'T7 never receives index 1 and must remain open (never published, no partial visibility)',
      largeTx: 'T10 20000 rows, prepared in 200-row batches, single short publish commit',
    },
  };
}
