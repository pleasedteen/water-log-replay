/**
 * Independent reference replayer.
 *
 * Deliberately does NOT import any merge/prepare/publish code from src/sync:
 * it re-implements transaction ordering and schema handling from the raw
 * fixture events so the integration tests have an independent oracle for
 * out-of-order delivery, duplicates and the v1 -> v2 migration.
 *
 * Rules applied (from the protocol spec, re-derived here):
 *  - a transaction becomes visible only when its commit marker AND all
 *    eventCount row events have been observed
 *  - row operations apply in `index` order; duplicate deliveries are ignored
 *  - newer txVersion wins per recordId; delete removes the record
 *  - v2 rows require the schema event; v1 rows observed afterwards map to
 *    methodCode='legacy'
 */

import { buildFixture } from '../src/fixtures/fixtures';
import type { Reading, RowEvent, SchemaVersion } from '../src/shared/protocol';

interface TxAccumulator {
  txId: string;
  txVersion: bigint;
  expected: number | null;
  rows: Map<number, RowEvent>;
  maxSchema: SchemaVersion;
  schemaEventSeen: boolean;
}

export interface ExpectedState {
  schemaVersion: SchemaVersion;
  /** recordId -> expected exposed reading (v2 shape when migrated) */
  records: Map<string, Reading>;
  publishedTxIds: string[];
  /** transactions that can never complete (e.g. permanently missing a row). */
  foreverOpen: string[];
}

export function referenceReplay(events = buildFixture().events): ExpectedState {
  const txns = new Map<string, TxAccumulator>();
  let schemaVersion: SchemaVersion = 1;
  const schemaDeclaredVersions = new Set<number>();

  const ensure = (txId: string, txVersion: bigint, schema: SchemaVersion): TxAccumulator => {
    let t = txns.get(txId);
    if (!t) {
      t = { txId, txVersion, expected: null, rows: new Map(), maxSchema: schema, schemaEventSeen: false };
      txns.set(txId, t);
    }
    t.maxSchema = Math.max(t.maxSchema, schema) as SchemaVersion;
    return t;
  };

  // Single pass over the arrival order (duplicates included).
  for (const ev of events) {
    if (ev.kind === 'schema') {
      schemaDeclaredVersions.add(ev.schemaVersion);
      if (ev.schemaVersion === 2) schemaVersion = 2;
      continue;
    }
    if (ev.kind === 'commit') {
      const t = ensure(ev.txId, BigInt(ev.txVersion), 1);
      t.expected = ev.eventCount;
      continue;
    }
    const t = ensure(ev.txId, BigInt(ev.txVersion), ev.schemaVersion);
    if (!t.rows.has(ev.index)) t.rows.set(ev.index, ev); // duplicate delivery ignored
  }

  const publishable: TxAccumulator[] = [];
  const foreverOpen: string[] = [];
  for (const t of txns.values()) {
    const complete = t.expected !== null && t.rows.size >= (t.expected as number);
    if (!complete) {
      foreverOpen.push(t.txId);
      continue;
    }
    if (t.maxSchema === 2 && !schemaDeclaredVersions.has(2)) continue; // unexplainable without schema
    publishable.push(t);
  }
  publishable.sort((a, b) => (a.txVersion < b.txVersion ? -1 : 1));

  // Per-record fold across published transactions, in commit order.
  const records = new Map<string, Reading>();
  const publishedTxIds: string[] = [];
  for (const t of publishable) {
    publishedTxIds.push(t.txId);
    const indexes = [...t.rows.keys()].sort((a, b) => a - b);
    for (const idx of indexes) {
      const row = t.rows.get(idx) as RowEvent;
      if (row.op === 'delete') {
        records.delete(row.recordId);
        continue;
      }
      const mirror = row.after as unknown as Record<string, unknown>;
      if (schemaVersion === 1) {
        records.set(row.recordId, {
          recordId: row.recordId,
          stationId: mirror.stationId as string,
          sampledAt: mirror.sampledAt as string,
          nitrate_mg_l: (mirror.nitrate_mg_l as number | null) ?? null,
        });
      } else {
        const legacy = row.schemaVersion === 1;
        records.set(row.recordId, {
          recordId: row.recordId,
          stationId: mirror.stationId as string,
          sampledAt: mirror.sampledAt as string,
          nitrateMgL: legacy ? ((mirror.nitrate_mg_l as number | null) ?? null) : ((mirror.nitrateMgL as number | null) ?? null),
          methodCode: legacy ? 'legacy' : (mirror.methodCode as string),
        });
      }
    }
  }

  return { schemaVersion, records, publishedTxIds, foreverOpen: foreverOpen.sort() };
}
