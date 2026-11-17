/**
 * SQLite-backed state store for the synchronization service.
 *
 * Durability model
 * ----------------
 * Everything the service needs to recover lives in one SQLite file (WAL mode):
 *   meta             - received offset, applied schema version, retry counters
 *   raw_events       - every received envelope event, deduped by offset PK
 *   tx_state         - one row per transaction (open -> prepared -> published)
 *   tx_rows          - row events grouped per transaction (deduped tx+index)
 *   record_versions  - STAGED row changes; invisible until the tx is published
 *
 * Publishing never copies rows again: a publish is one short INSERT INTO
 * published_txns. Reads compute the latest visible state by joining
 * record_versions against published_txns.
 */

import { DatabaseSync } from 'node:sqlite';
import { randomUUID } from 'node:crypto';
import type { LogEvent, Reading, RowEvent, SchemaVersion, SyncStatus } from '../shared/protocol';

export const MAX_PREPARE_ROWS = 200;

type Stmt = ReturnType<DatabaseSync['prepare']>;

export interface StoredRow {
  txId: string;
  txVersion: string;
  idx: number;
  schemaVersion: SchemaVersion;
  op: 'upsert' | 'delete';
  recordId: string;
  /** original wire mirror (JSON), absent for deletes. */
  payload: string | null;
}

export class SyncStore {
  readonly db: DatabaseSync;
  private stmts!: Record<string, Stmt>;

  constructor(file: string) {
    // Ensure the parent directory exists (fresh checkout has no data/ folder).
    const parent = require('node:path').dirname(file) as string;
    require('node:fs').mkdirSync(parent, { recursive: true });
    this.db = new DatabaseSync(file);
    this.db.exec('pragma journal_mode=WAL');
    this.db.exec('pragma synchronous=FULL');
    this.db.exec('pragma foreign_keys=ON');
    this.initSchema();
    this.prepareStatements();
  }

  close(): void {
    this.db.close();
  }

  private initSchema(): void {
    this.db.exec(`
      create table if not exists meta (
        id integer primary key check (id = 1),
        received_offset text not null default '0',
        schema_version integer not null default 1,
        retry_count integer not null default 0,
        last_error text
      );
      insert or ignore into meta (id, received_offset, schema_version, retry_count) values (1, '0', 1, 0);

      create table if not exists raw_events (
        offset_val text primary key,
        event_json text not null
      ) without rowid;

      create table if not exists tx_state (
        tx_id text primary key,
        tx_version text not null,
        schema_version integer not null,
        status text not null check (status in ('open','prepared','published')),
        expected_count integer,
        received_count integer not null default 0,
        prepared_count integer not null default 0,
        commit_seen integer not null default 0,
        created_at text not null,
        updated_at text not null
      ) without rowid;
      create index if not exists idx_tx_state_status on tx_state(status);

      create table if not exists tx_rows (
        tx_id text not null,
        idx integer not null,
        op text not null,
        record_id text not null,
        schema_version integer not null,
        payload_json text,
        staged integer not null default 0,
        primary key (tx_id, idx)
      ) without rowid;

      -- v1 physical shape: old column name, no methodCode.
      create table if not exists record_versions (
        record_id text not null,
        tx_version text not null,
        op text not null,
        station_id text,
        sampled_at text,
        nitrate_mg_l real,
        primary key (record_id, tx_version)
      ) without rowid;

      create table if not exists published_txns (
        tx_id text primary key,
        tx_version text not null,
        published_at text not null
      ) without rowid;
      create index if not exists idx_published_version on published_txns(tx_version);
    `);
    // Idempotent upgrade for database files created before tx_rows.staged existed.
    const cols = this.db.prepare('pragma table_info(tx_rows)').all() as Array<{ name: string }>;
    if (!cols.some((c) => c.name === 'staged')) {
      this.db.exec('alter table tx_rows add column staged integer not null default 0');
    }
  }

  private prepareStatements(): void {
    const s = this.stmts ?? {};
    s.getMeta = this.db.prepare('select * from meta where id=1');
    s.setOffset = this.db.prepare('update meta set received_offset=? where id=1');
    s.setSchema = this.db.prepare('update meta set schema_version=? where id=1');
    s.incRetry = this.db.prepare('update meta set retry_count=retry_count+1 where id=1');
    s.setError = this.db.prepare('update meta set last_error=? where id=1');
    s.insertRaw = this.db.prepare('insert or ignore into raw_events(offset_val, event_json) values(?,?)');
    s.upsertTx = this.db.prepare(`
      insert into tx_state(tx_id, tx_version, schema_version, status, expected_count, received_count, commit_seen, created_at, updated_at)
      values(@txId, @txVersion, @schemaVersion, 'open', null, 0, 0, @now, @now)
      on conflict(tx_id) do update set
        tx_version=excluded.tx_version,
        schema_version=max(tx_state.schema_version, excluded.schema_version),
        updated_at=@now
    `);
    s.setExpected = this.db.prepare('update tx_state set expected_count=?, commit_seen=1, updated_at=? where tx_id=?');
    s.touchTx = this.db.prepare('update tx_state set updated_at=? where tx_id=?');
    s.insertRow = this.db.prepare(`
      insert or ignore into tx_rows(tx_id, idx, op, record_id, schema_version, payload_json)
      values(?,?,?,?,?,?)
    `);
    s.countRows = this.db.prepare('select count(*) c from tx_rows where tx_id=?');
    s.setReceived = this.db.prepare('update tx_state set received_count=? where tx_id=?');
    s.getTx = this.db.prepare('select * from tx_state where tx_id=?');
    s.listOpen = this.db.prepare("select * from tx_state where status='open' order by tx_version");
    s.listPrepared = this.db.prepare("select * from tx_state where status='prepared' order by tx_version");
    s.pendingRows = this.db.prepare(`
      select r.* from tx_rows r
      where r.tx_id=? and r.staged=0
      order by r.idx
      limit ?
    `);
    s.markRowStaged = this.db.prepare('update tx_rows set staged=1 where tx_id=? and idx=?');
    s.countStaged = this.db.prepare('select count(*) c from tx_rows where tx_id=? and staged=1');
    s.markPrepared = this.db.prepare("update tx_state set status='prepared', prepared_count=?, updated_at=? where tx_id=?");
    s.preparedCount = this.db.prepare('select count(*) c from tx_rows where tx_id=?');
    s.insertPublished = this.db.prepare('insert or ignore into published_txns(tx_id, tx_version, published_at) values(?,?,?)');
    s.markPublished = this.db.prepare("update tx_state set status='published', updated_at=? where tx_id=?");
    s.txVersionOf = this.db.prepare('select tx_version from tx_state where tx_id=?');
    this.stmts = s;
    // Staging statements reference physical columns whose names change during
    // the v1->v2 migration; compile the variant matching the on-disk schema so
    // a process reopening an already-migrated database does not crash.
    this.buildStageStatements(this.getSchemaVersion());
  }

  /** Compile staging statements for the currently applied physical schema. */
  private buildStageStatements(version: SchemaVersion): void {
    if (version === 1) {
      this.stmts.stageUpsertV1 = this.db.prepare(`
        insert into record_versions(record_id, tx_version, op, station_id, sampled_at, nitrate_mg_l)
        values(?,?,'upsert',?,?,?)
        on conflict(record_id, tx_version) do update set
          op='upsert', station_id=excluded.station_id, sampled_at=excluded.sampled_at, nitrate_mg_l=excluded.nitrate_mg_l
      `);
      this.stmts.stageDeleteV1 = this.db.prepare(`
        insert into record_versions(record_id, tx_version, op, station_id, sampled_at, nitrate_mg_l)
        values(?,?,'delete',null,null,null)
        on conflict(record_id, tx_version) do update set
          op='delete', station_id=null, sampled_at=null, nitrate_mg_l=null
      `);
    } else {
      this.rebuildV2Statements();
    }
  }

  // ------------------------------------------------------------ meta ----
  getReceivedOffset(): string {
    return (this.stmts.getMeta.get() as { received_offset: string }).received_offset;
  }
  getSchemaVersion(): SchemaVersion {
    return (this.stmts.getMeta.get() as { schema_version: SchemaVersion }).schema_version;
  }
  setReceivedOffset(offset: string): void {
    this.stmts.setOffset.run(offset);
  }
  recordRetry(): void {
    this.stmts.incRetry.run();
  }
  setLastError(message: string | null): void {
    this.stmts.setError.run(message);
  }

  getStatus(running: boolean): SyncStatus {
    const meta = this.stmts.getMeta.get() as {
      received_offset: string;
      schema_version: SchemaVersion;
      retry_count: number;
      last_error: string | null;
    };
    const open = (
      this.db.prepare("select count(*) c from tx_state where status='open'").get() as { c: number }
    ).c;
    const prepared = (
      this.db.prepare("select count(*) c from tx_state where status='prepared'").get() as { c: number }
    ).c;
    const published = (
      this.db.prepare('select count(*) c from published_txns').get() as { c: number }
    ).c;
    return {
      receivedOffset: meta.received_offset,
      schemaVersion: meta.schema_version,
      openTransactions: open,
      preparedTransactions: prepared,
      publishedTransactions: published,
      retryCount: meta.retry_count,
      lastError: meta.last_error,
      running,
    };
  }

  /**
   * Durably persist one fetched page atomically.
   * Raw events, row grouping and tx bookkeeping all commit together, then the
   * received offset advances inside the SAME transaction - either the whole
   * page survives a crash or none of it does (no double business effect).
   */
  persistPage(entries: Array<{ offset: string; event: LogEvent }>, nextOffset: string): void {
    const now = new Date().toISOString();
    this.db.exec('begin immediate');
    try {
      for (const { offset, event } of entries) {
        this.stmts.insertRaw.run(offset, JSON.stringify(event));
        if (event.kind === 'row') this.applyRowEvent(event, now);
        else if (event.kind === 'commit') this.applyCommitEvent(event, now);
        // schema events are tracked solely via raw_events + applySchemaEvent
      }
      this.setReceivedOffset(nextOffset);
      this.db.exec('commit');
    } catch (err) {
      this.db.exec('rollback');
      throw err;
    }
  }

  private applyRowEvent(ev: RowEvent, now: string): void {
    this.stmts.upsertTx.run({
      txId: ev.txId,
      txVersion: ev.txVersion,
      schemaVersion: ev.schemaVersion,
      now,
    });
    this.stmts.insertRow.run(
      ev.txId,
      ev.index,
      ev.op,
      ev.recordId,
      ev.schemaVersion,
      ev.after === undefined ? null : JSON.stringify(ev.after),
    );
    const c = (this.stmts.countRows.get(ev.txId) as { c: number }).c;
    this.stmts.setReceived.run(c, ev.txId);
    this.stmts.touchTx.run(now, ev.txId);
  }

  private applyCommitEvent(ev: { txId: string; txVersion: string; eventCount: number }, now: string): void {
    // A commit may arrive before any row of that transaction.
    this.stmts.upsertTx.run({
      txId: ev.txId,
      txVersion: ev.txVersion,
      schemaVersion: 1, // corrected later when rows arrive; upsert keeps the higher known value below
      now,
    });
    this.stmts.setExpected.run(ev.eventCount, now, ev.txId);
  }

  /** Read every persisted schema declaration in offset order and apply up to v2. */
  applyDueSchemaEvents(): Array<{ offset: string; event: Extract<LogEvent, { kind: 'schema' }> }> {
    const applied: Array<{ offset: string; event: Extract<LogEvent, { kind: 'schema' }> }> = [];
    const rows = this.db
      .prepare("select offset_val, event_json from raw_events where json_extract(event_json,'$.kind')='schema' order by cast(offset_val as integer)")
      .all() as Array<{ offset_val: string; event_json: string }>;
    for (const r of rows) {
      const event = JSON.parse(r.event_json) as Extract<LogEvent, { kind: 'schema' }>;
      if (event.schemaVersion > this.getSchemaVersion()) {
        this.migrateToV2();
        applied.push({ offset: r.offset_val, event });
      }
    }
    return applied;
  }

  /**
   * Physical schema migration (not just response renaming):
   *  - record_versions gains the real column methodCode and nitrateMgL
   *  - nitrate_mg_l column is physically renamed; v1 backlog rows get legacy
   *  - tx_state rows tagged v1 are promoted so their later staging uses v2
   */
  migrateToV2(): void {
    const current = this.getSchemaVersion();
    if (current >= 2) return;
    this.db.exec('begin immediate');
    try {
      this.db.exec('alter table record_versions rename column nitrate_mg_l to nitrateMgL');
      // Nullable at the storage level because delete tombstones carry no row
      // mirror; the NOT NULL v2 contract for live rows is enforced when upserts
      // are staged (methodCode always present, legacy rows get 'legacy').
      this.db.exec('alter table record_versions add column method_code text');
      // Already-staged v1 rows: legacy method, values preserved.
      this.db.exec("update record_versions set method_code='legacy' where method_code='legacy'");
      // v1 transactions whose rows arrive after the migration stage as v2-legacy.
      this.db.exec('update tx_state set schema_version=2 where schema_version=1');
      this.stmts.setSchema.run(2);
      this.db.exec('commit');
      // prepared statements reference renamed columns; rebuild them
      this.rebuildV2Statements();
    } catch (err) {
      this.db.exec('rollback');
      throw err;
    }
  }

  private rebuildV2Statements(): void {
    this.stmts.stageUpsertV1 = this.db.prepare(`
      insert into record_versions(record_id, tx_version, op, station_id, sampled_at, nitrateMgL, method_code)
      values(?,?,'upsert',?,?,?,?)
      on conflict(record_id, tx_version) do update set
        op='upsert', station_id=excluded.station_id, sampled_at=excluded.sampled_at,
        nitrateMgL=excluded.nitrateMgL, method_code=excluded.method_code
    `);
    this.stmts.stageDeleteV1 = this.db.prepare(`
      insert into record_versions(record_id, tx_version, op, station_id, sampled_at, nitrateMgL, method_code)
      values(?,?,'delete',null,null,null,null)
      on conflict(record_id, tx_version) do update set
        op='delete', station_id=null, sampled_at=null, nitrateMgL=null, method_code=null
    `);
  }

  /**
   * Prepare at most MAX_PREPARE_ROWS pending row changes.
   * Returns diagnostics about what was staged. Never touches published data,
   * so in-progress staging is invisible to business queries.
   */
  prepareBatch(faultAfter?: number): { txId: string | null; rows: number; finished: boolean } {
    const candidate = this.nextPrepareCandidate();
    if (!candidate) return { txId: null, rows: 0, finished: false };
    const txVersion = (this.stmts.txVersionOf.get(candidate.tx_id) as { tx_version: string }).tx_version;
    const pending = this.stmts.pendingRows.all(candidate.tx_id, MAX_PREPARE_ROWS) as Array<{
      tx_id: string;
      idx: number;
      op: string;
      record_id: string;
      schema_version: number;
      payload_json: string | null;
    }>;
    if (pending.length === 0) return { txId: null, rows: 0, finished: false };

    this.db.exec('begin immediate');
    try {
      let staged = 0;
      for (const r of pending) {
        if (r.op === 'delete') {
          this.stmts.stageDeleteV1.run(r.record_id, txVersion);
        } else {
          const mirror = JSON.parse(r.payload_json as string) as {
            stationId: string;
            sampledAt: string;
            nitrate_mg_l?: number | null;
            nitrateMgL?: number | null;
            methodCode?: string;
          };
          if (this.getSchemaVersion() === 1) {
            this.stmts.stageUpsertV1.run(
              r.record_id,
              txVersion,
              mirror.stationId,
              mirror.sampledAt,
              mirror.nitrate_mg_l ?? null,
            );
          } else {
            // v2 wire shape, or v1 backlog row arriving after migration -> legacy
            const isV1 = r.schema_version === 1;
            const nitrate = isV1 ? mirror.nitrate_mg_l : mirror.nitrateMgL;
            const method = isV1 ? 'legacy' : (mirror.methodCode as string);
            this.stmts.stageUpsertV1.run(r.record_id, txVersion, mirror.stationId, mirror.sampledAt, nitrate ?? null, method);
          }
        }
        // Per-row watermark: repeated operations on the same recordId inside
        // one tx collapse onto one record_versions row, so progress must be
        // tracked on tx_rows rather than by counting staged version rows.
        this.stmts.markRowStaged.run(candidate.tx_id, r.idx);
        staged += 1;
        if (faultAfter !== undefined && staged === faultAfter) {
          // Simulate a hard process crash mid-prepare: commit what is durable so
          // far (separate batches), then let the caller perform process.exit.
          this.db.exec('commit');
          return { txId: candidate.tx_id, rows: staged, finished: false };
        }
      }
      const totalRows = (this.stmts.preparedCount.get(candidate.tx_id) as { c: number }).c;
      const stagedRows = (this.stmts.countStaged.get(candidate.tx_id) as { c: number }).c;
      const now = new Date().toISOString();
      // Only mark the transaction prepared once ALL its rows are staged; a
      // large transaction spans many <=200-row batches and stays 'open' across
      // them (and across possible restarts) in the meantime.
      if (stagedRows >= totalRows) {
        this.stmts.markPrepared.run(totalRows, now, candidate.tx_id);
      } else {
        this.db.prepare('update tx_state set prepared_count=?, updated_at=? where tx_id=?').run(stagedRows, now, candidate.tx_id);
      }
      this.db.exec('commit');
      return { txId: candidate.tx_id, rows: staged, finished: stagedRows >= totalRows };
    } catch (err) {
      this.db.exec('rollback');
      throw err;
    }
  }

  /** Cumulative number of rows of one transaction already staged (durable watermark). */
  stagedRowCount(txId: string): number {
    return (this.stmts.countStaged.get(txId) as { c: number }).c;
  }

  /** Durable per-transaction row used by tests/probes to observe partial prepare. */
  getTxState(txId: string): {
    tx_id: string;
    tx_version: string;
    status: 'open' | 'prepared' | 'published';
    expected_count: number | null;
    received_count: number;
    prepared_count: number;
  } | null {
    const row = this.stmts.getTx.get(txId) as
      | {
          tx_id: string;
          tx_version: string;
          status: 'open' | 'prepared' | 'published';
          expected_count: number | null;
          received_count: number;
          prepared_count: number;
        }
      | undefined;
    return row ?? null;
  }

  /**
   * Eligible = all row events received AND commit seen with matching count AND
   * schema interpretable (v2 rows require schema v2 to be applied).
   */
  nextPrepareCandidate(): { tx_id: string; schema_version: number } | null {
    const rows = this.stmts.listOpen.all() as Array<{
      tx_id: string;
      schema_version: number;
      expected_count: number | null;
      received_count: number;
    }>;
    for (const r of rows) {
      if (r.expected_count === null) continue; // no commit marker yet
      if (r.received_count < r.expected_count) continue; // still missing rows (e.g. TX-0007)
      if (r.schema_version === 2 && this.getSchemaVersion() === 1) continue; // schema event not yet explainable
      return { tx_id: r.tx_id, schema_version: r.schema_version };
    }
    return null;
  }

  /**
   * Publish the oldest fully-prepared transaction in one short commit.
   * Returns the published txId, or null when nothing is prepared. Publishing
   * one transaction at a time lets the engine honor pause boundaries between
   * durable publish units instead of flushing every prepared tx in one go.
   */
  publishOne(): string | null {
    const prepared = this.stmts.listPrepared.get() as
      | { tx_id: string; tx_version: string }
      | undefined;
    if (!prepared) return null;
    const now = new Date().toISOString();
    this.db.exec('begin immediate');
    try {
      this.stmts.insertPublished.run(prepared.tx_id, prepared.tx_version, now);
      this.stmts.markPublished.run(now, prepared.tx_id);
      this.db.exec('commit');
    } catch (err) {
      this.db.exec('rollback');
      throw err;
    }
    return prepared.tx_id;
  }

  /** Publish all prepared transactions; each publish is one short tx. */
  publishReady(fault: { before?: boolean; after?: boolean } = {}): string[] {
    const published: string[] = [];
    const prepared = this.stmts.listPrepared.all() as Array<{ tx_id: string; tx_version: string }>;
    for (const p of prepared) {
      if (fault.before) return published; // caller exits before this commit
      const now = new Date().toISOString();
      this.db.exec('begin immediate');
      try {
        this.stmts.insertPublished.run(p.tx_id, p.tx_version, now);
        this.stmts.markPublished.run(now, p.tx_id);
        this.db.exec('commit');
      } catch (err) {
        this.db.exec('rollback');
        throw err;
      }
      published.push(p.tx_id);
      if (fault.after) return published; // caller exits immediately after first publish commit
    }
    return published;
  }

  hasPrepared(): boolean {
    return (this.db.prepare("select count(*) c from tx_state where status='prepared'").get() as { c: number }).c > 0;
  }

  preparedTxIds(): string[] {
    return (this.stmts.listPrepared.all() as Array<{ tx_id: string }>).map((r) => r.tx_id);
  }

  // ------------------------------------------------------------ reads ----
  private latestVisibleSql(): string {
    return `
      with visible as (
        select rv.*,
               row_number() over (partition by rv.record_id order by cast(rv.tx_version as integer) desc) rn
        from record_versions rv
        join published_txns p on p.tx_version = rv.tx_version
      )
      select * from visible where rn = 1 and op = 'upsert'
    `;
  }

  getReading(recordId: string): Reading | null {
    const row = this.db
      .prepare(`${this.latestVisibleSql()} and record_id = ?`)
      .get(recordId) as
      | { record_id: string; station_id: string; sampled_at: string; nitrateMgL: number | null; method_code: string | null }
      | undefined;
    if (!row) return null;
    return this.shape(row);
  }

  listReadings(limit: number, afterRecordId: string | null): { items: Reading[]; nextRecordId: string | null } {
    const sql = `${this.latestVisibleSql()} ${afterRecordId ? 'and record_id > ?' : ''} order by record_id limit ?`;
    const rows = (
      afterRecordId
        ? this.db.prepare(sql).all(afterRecordId, limit + 1)
        : this.db.prepare(sql).all(limit + 1)
    ) as Array<{
      record_id: string;
      station_id: string;
      sampled_at: string;
      nitrateMgL: number | null;
      method_code: string | null;
    }>;
    const page = rows.slice(0, limit);
    const nextRecordId = rows.length > limit ? page[page.length - 1].record_id : null;
    return { items: page.map((r) => this.shape(r)), nextRecordId };
  }

  private shape(row: {
    record_id: string;
    station_id: string;
    sampled_at: string;
    nitrateMgL: number | null;
    method_code: string | null;
  }): Reading {
    if (this.getSchemaVersion() === 1) {
      // Physical column is renamed only during the v2 migration; on v1 the
      // stored value is read from the original nitrate_mg_l column.
      const value =
        'nitrate_mg_l' in row
          ? (row as unknown as { nitrate_mg_l: number | null }).nitrate_mg_l
          : row.nitrateMgL;
      return {
        recordId: row.record_id,
        stationId: row.station_id,
        sampledAt: row.sampled_at,
        nitrate_mg_l: value,
      };
    }
    return {
      recordId: row.record_id,
      stationId: row.station_id,
      sampledAt: row.sampled_at,
      nitrateMgL: row.nitrateMgL,
      methodCode: row.method_code ?? 'legacy',
    };
  }

  /** Debug/diagnostic snapshot used by tests and the /diagnostics endpoint. */
  diagnostics(): Record<string, unknown> {
    const txs = this.db
      .prepare('select tx_id, tx_version, status, expected_count, received_count, prepared_count from tx_state order by tx_version')
      .all();
    const staged = (this.db.prepare('select count(*) c from record_versions').get() as { c: number }).c;
    return {
      meta: this.stmts.getMeta.get(),
      transactions: txs,
      stagedRows: staged,
      publishedRows: this.db
        .prepare(
          `select count(*) c from record_versions rv join published_txns p on p.tx_version=rv.tx_version where rv.op='upsert'`,
        )
        .get(),
      instance: randomUUID(),
    };
  }
}
