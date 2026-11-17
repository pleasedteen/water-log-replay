/**
 * Wire protocol types shared by the source simulator, the sync service,
 * the reference replayer and the test suite.
 *
 * Every offset / txVersion is transported as a DECIMAL STRING. Internally we
 * use bigint (and store TEXT in SQLite) so values beyond JS safe-integer
 * range (the log starts near Number.MAX_SAFE_INTEGER) stay exact.
 */

export const SCHEMA_V1 = 1 as const;
export const SCHEMA_V2 = 2 as const;
export type SchemaVersion = 1 | 2;

/** Row mirror as carried on the wire for schemaVersion 1. */
export interface ReadingV1 {
  recordId: string;
  stationId: string;
  sampledAt: string;
  /** milligram per litre; 0 is a real measurement, null means no valid reading this time. */
  nitrate_mg_l: number | null;
}

/** Row mirror as carried on the wire for schemaVersion 2. */
export interface ReadingV2 {
  recordId: string;
  stationId: string;
  sampledAt: string;
  nitrateMgL: number | null;
  methodCode: string;
}

export type RowMirror = ReadingV1 | ReadingV2;

export interface RowEvent {
  kind: 'row';
  txId: string;
  txVersion: string;
  index: number;
  schemaVersion: SchemaVersion;
  op: 'upsert' | 'delete';
  recordId: string;
  /** present only on upsert; delete events never carry `after`. */
  after?: RowMirror;
}

export interface CommitEvent {
  kind: 'commit';
  txId: string;
  txVersion: string;
  eventCount: number;
}

export interface SchemaEvent {
  kind: 'schema';
  /** monotonically increasing schema declaration id (string offset position is assigned by the source) */
  schemaVersion: SchemaVersion;
  changes: Array<{
    type: 'renameColumn';
    table: string;
    from: string;
    to: string;
  } | {
    type: 'addColumn';
    table: string;
    column: string;
    ddlType: string;
    nullable: boolean;
    defaultForExisting: string;
  }>;
}

export type LogEvent = RowEvent | CommitEvent | SchemaEvent;

/**
 * Transport annotation added by the source simulator: the exact source
 * position of an event. Present on every wire event so overlap pages can be
 * deduped by offset on the receiving side.
 */
export type DeliveredEvent = LogEvent & { _offset: string };

export interface LogEnvelope {
  events: LogEvent[];
  /** decimal string; position immediately after the last (pre-overlap) event of this page. */
  nextOffset: string;
  hasMore: boolean;
}

export interface Reading {
  recordId: string;
  stationId: string;
  sampledAt: string;
  /** key name depends on the currently exposed schema version. */
  nitrate_mg_l?: number | null;
  nitrateMgL?: number | null;
  methodCode?: string;
}

export interface ListResponse {
  schemaVersion: SchemaVersion;
  limit: number;
  nextRecordId: string | null;
  items: Reading[];
}

export interface SyncStatus {
  /** last durable received log position (decimal string). */
  receivedOffset: string;
  /** schema version currently exposed on the read API and physically applied in SQLite. */
  schemaVersion: SchemaVersion;
  /** tx_state rows that are still waiting for rows / a commit marker. */
  openTransactions: number;
  /** transactions fully staged into record_versions but not yet published. */
  preparedTransactions: number;
  /** transactions visible to business queries. */
  publishedTransactions: number;
  /** cumulative retry count (HTTP 503 / network errors / backoffs). */
  retryCount: number;
  lastError: string | null;
  running: boolean;
}

/** One-shot fault injection points used by the integration tests. */
export type FaultPoint =
  | 'after_receive_persist'
  | 'mid_prepare_big_tx'
  | 'before_publish_commit'
  | 'after_publish_commit';
