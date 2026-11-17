/**
 * Synchronization engine: pull loop, retry/backoff, prepare/publish pipeline,
 * one-shot fault injection and structured diagnostics.
 *
 * Streaming guarantee: the source is read one page (at most `pageLimit`
 * events) at a time and prepare batches are capped at MAX_PREPARE_ROWS rows.
 * Neither the whole log nor the 20k-row transaction is ever materialized in
 * memory at once.
 *
 * Pause semantics
 * ---------------
 * A pause is honored at DURABLE-UNIT boundaries only. A durable unit is one
 * of:
 *   - a page persistence transaction (persistPage)
 *   - one <=200-row prepare batch commit (prepareBatch)
 *   - one short per-transaction publish commit (publishOne)
 *
 * Once pause has been requested, the unit currently in flight is allowed to
 * finish and commit; the engine then starts no further receive, prepare or
 * publish unit. Prepared-but-unpublished and partially-prepared state stays
 * on disk and is resumed by the next start (or by a fresh process reopening
 * the same SQLite file). Pause never rolls staged work back and never
 * re-fetches already-persisted data.
 *
 * start()/pause() calls are serialized through a control promise chain so
 * concurrent requests take effect one at a time in arrival order; there can
 * never be two loops and a pause can never return before the loop has
 * actually stopped.
 */

import { SyncStore, MAX_PREPARE_ROWS } from './store';
import type { DeliveredEvent, FaultPoint, LogEnvelope } from '../shared/protocol';

/** Persist this many pages before the after_receive_persist fault exits. */
const RECEIVE_FAULT_AFTER_PAGES = 2;

/** Configuration of the configurable mid-prepare fault point. */
export interface MidPrepareFaultConfig {
  /** only this transaction triggers the fault */
  txId: string;
  /** exit once this many rows of the target tx are cumulatively staged */
  afterRows: number;
}

export interface EngineOptions {
  sourceBaseUrl: string;
  pageLimit: number;
  /** initial backoff ms after HTTP 503 / network errors; doubles, capped. */
  baseBackoffMs: number;
  maxBackoffMs: number;
  /**
   * Yield the event loop for this many ms after every prepare batch. Zero in
   * production; tests use it to keep the read API responsive and observable
   * while a 20k-row transaction is being prepared in 200-row chunks.
   */
  prepareBatchDelayMs: number;
  fault?: FaultPoint | null;
  /** target transaction / cumulative staged rows for mid_prepare_big_tx */
  midPrepare?: MidPrepareFaultConfig | null;
  /** structured log sink (injectable for tests). */
  log?: (line: Record<string, unknown>) => void;
}

type PrepareBatchResult = ReturnType<SyncStore['prepareBatch']>;

export class SyncEngine {
  private store: SyncStore;
  private opts: Required<Omit<EngineOptions, 'fault' | 'midPrepare'>> & {
    fault: FaultPoint | null;
    midPrepare: MidPrepareFaultConfig | null;
  };
  private running = false;
  private paused = true;
  private loopPromise: Promise<void> | null = null;
  private wakeWaiter: (() => void) | null = null;
  private faultConsumed = false;
  /** Serializes concurrent start/pause requests in arrival order. */
  private controlChain: Promise<unknown> = Promise.resolve();
  private pagesPersisted = 0;

  constructor(store: SyncStore, options: EngineOptions) {
    this.store = store;
    this.opts = {
      pageLimit: options.pageLimit,
      baseBackoffMs: options.baseBackoffMs,
      maxBackoffMs: options.maxBackoffMs,
      prepareBatchDelayMs: options.prepareBatchDelayMs,
      sourceBaseUrl: options.sourceBaseUrl,
      fault: options.fault ?? null,
      midPrepare: options.midPrepare ?? null,
      log: options.log ?? ((line) => console.log(JSON.stringify(line))),
    };
  }

  isRunning(): boolean {
    return this.running;
  }
  isPaused(): boolean {
    return this.paused;
  }

  /**
   * Start the pull loop. Idempotent: returns false when a loop is already
   * running and never creates a second one. Concurrent start/pause calls are
   * serialized, so a start arriving while a pause is still settling waits for
   * that pause and then starts exactly one fresh loop.
   */
  start(): Promise<boolean> {
    const step = this.controlChain.then(() => {
      if (this.running) return false; // duplicate start never creates a second loop
      this.running = true;
      this.paused = false;
      this.loopPromise = this.loop().catch((err) => {
        this.opts.log({ svc: 'sync', level: 'error', event: 'loop-terminated', error: String(err) });
      });
      this.opts.log({ svc: 'sync', event: 'sync-start', receivedOffset: this.store.getReceivedOffset() });
      return true;
    });
    // Keep the chain alive even if a caller's awaited promise rejects.
    this.controlChain = step.catch(() => undefined);
    return step;
  }

  /**
   * Request a pause and wait until the loop has stopped at a durable-unit
   * boundary. When this settles, running=false and every durable position
   * (received offset, prepare watermarks, published set) is stable. Multiple
   * concurrent pauses all wait for the same stop; none returns early.
   */
  pause(): Promise<void> {
    const step = this.controlChain.then(async () => {
      this.paused = true;
      if (this.wakeWaiter) this.wakeWaiter(); // abort any backoff/sleep immediately
      const loop = this.loopPromise;
      this.loopPromise = null;
      if (loop) await loop; // resolve only after the loop actually stopped
      this.running = false;
      this.opts.log({ svc: 'sync', event: 'sync-paused', receivedOffset: this.store.getReceivedOffset() });
    });
    this.controlChain = step.catch(() => undefined);
    return step;
  }

  private async loop(): Promise<void> {
    this.opts.log({ svc: 'sync', event: 'loop-start', receivedOffset: this.store.getReceivedOffset() });
    let backoff = this.opts.baseBackoffMs;
    while (!this.paused) {
      let page: LogEnvelope;
      try {
        page = await this.fetchPage(this.store.getReceivedOffset());
        backoff = this.opts.baseBackoffMs; // success resets the backoff window
      } catch (err) {
        this.store.recordRetry();
        this.store.setLastError(err instanceof Error ? err.message : String(err));
        this.opts.log({ svc: 'sync', level: 'warn', event: 'fetch-retry', retryInMs: backoff, error: String(err) });
        await this.interruptibleSleep(backoff);
        if (this.paused) break; // pause ends the backoff wait promptly
        backoff = Math.min(backoff * 2, this.opts.maxBackoffMs);
        continue;
      }

      // A pause requested while the fetch was in flight: the fetch is not a
      // durable unit, so stop before persisting anything. The same events are
      // re-fetched from the unchanged durable offset after resume.
      if (this.paused) break;

      const entries = page.events.map((event) => ({
        // Overlap pages repeat events whose offset is <= the prior tail;
        // the simulator annotates each event with its real transport offset.
        offset: (event as DeliveredEvent)._offset,
        event,
      }));
      if (entries.length > 0) {
        // One durable unit: the whole page commits atomically with the offset.
        this.store.persistPage(entries, page.nextOffset);
        this.store.setLastError(null);
        this.opts.log({
          svc: 'sync',
          event: 'page-persisted',
          rows: entries.length,
          nextOffset: page.nextOffset,
          hasMore: page.hasMore,
        });
        this.maybeReceiveFault();
      }

      // Boundary: once the in-flight persist unit is durable, a pending pause
      // stops the loop before schema work / prepare / publish.
      if (this.paused) break;

      // Schema declarations may arrive late; apply before evaluating candidates.
      const applied = this.store.applyDueSchemaEvents();
      for (const a of applied) this.opts.log({ svc: 'sync', event: 'schema-applied', schemaVersion: a.event.schemaVersion });

      // Prepare + publish whatever is currently completable, in small units so
      // reads stay responsive and memory stays bounded. drainPipeline itself
      // honors pause at every durable-unit boundary.
      await this.drainPipeline();
      if (this.paused) break;

      if (!page.hasMore && entries.length === 0) {
        // Caught up. Sleep briefly but remain interruptible by pause.
        await this.interruptibleSleep(50);
      }
    }
    this.opts.log({ svc: 'sync', event: 'loop-paused', receivedOffset: this.store.getReceivedOffset() });
  }

  private async drainPipeline(): Promise<void> {
    // ---- Phase 1: prepare in <=200-row durable batches --------------------
    // A large tx spans many batches; when its current batch is exhausted we
    // keep looping so other fully-received transactions can be prepared
    // instead of stalling behind the in-progress one. The pause flag is
    // re-checked between every batch: no new batch starts once pause is set,
    // and a partially prepared large tx stays 'open' on disk for resume.
    while (!this.paused) {
      const candidate = this.store.nextPrepareCandidate();
      if (!candidate) break;
      const cut = this.midPrepareCut(candidate.tx_id);
      const result: PrepareBatchResult = this.store.prepareBatch(cut ?? undefined);
      if (!result.txId) break;
      this.opts.log({
        svc: 'sync',
        event: result.finished ? 'tx-prepared' : 'prepare-batch',
        txId: result.txId,
        rows: result.rows,
        batchCap: MAX_PREPARE_ROWS,
      });
      if (this.consumeMidPrepareFault(candidate.tx_id, result)) return; // process exits (42)
      // Yield between batches: keeps the read API responsive and, when
      // configured, widens the prepare window for test probes. The sleep is
      // interruptible, so pause also cuts this wait short.
      if (this.opts.prepareBatchDelayMs > 0) await this.interruptibleSleep(this.opts.prepareBatchDelayMs);
      else await new Promise((r) => setImmediate(r));
    }

    // ---- Phase 2: publish, one short commit per prepared tx ---------------
    // Boundary before every publish: after pause, already-prepared
    // transactions are NOT published and remain staged in the database.
    while (!this.paused && this.store.hasPrepared()) {
      if (this.opts.fault === 'before_publish_commit' && !this.faultConsumed) {
        // Fire as soon as a transaction is fully staged, so the restart must
        // reconcile prepared-but-unpublished state itself.
        this.fireFault('before_publish_commit', { prepared: this.store.preparedTxIds().length });
        return;
      }
      const txId = this.store.publishOne();
      if (txId === null) break;
      this.opts.log({ svc: 'sync', event: 'tx-published', txId, via: 'short-commit' });
      if (this.opts.fault === 'after_publish_commit' && !this.faultConsumed) {
        // Fire after the first publish commit; the restart proves the already
        // committed publish is not duplicated while remaining txs still flow.
        this.fireFault('after_publish_commit', { published: 1 });
        return;
      }
    }
  }

  /**
   * Compute the early-commit row count for the current batch so the
   * configurable mid-prepare fault lands exactly on the configured cumulative
   * staged-row count. Returns null when the fault must not fire here.
   */
  private midPrepareCut(txId: string): number | null {
    if (this.opts.fault !== 'mid_prepare_big_tx' || this.faultConsumed) return null;
    const cfg = this.opts.midPrepare;
    if (!cfg || txId !== cfg.txId) return null;
    const already = this.store.stagedRowCount(txId);
    if (already >= cfg.afterRows) return null; // defensive: should have fired earlier
    const remaining = cfg.afterRows - already;
    return remaining < MAX_PREPARE_ROWS ? remaining : MAX_PREPARE_ROWS;
  }

  /**
   * Fire the mid-prepare fault once the target transaction reaches the
   * configured cumulative prepare row count. Returns true when the process
   * exit has been scheduled. Already-staged batches remain durable.
   */
  private consumeMidPrepareFault(txId: string, result: PrepareBatchResult): boolean {
    if (this.opts.fault !== 'mid_prepare_big_tx' || this.faultConsumed) return false;
    const cfg = this.opts.midPrepare;
    if (!cfg || txId !== cfg.txId) return false;
    const cumulative = this.store.stagedRowCount(txId);
    if (cumulative < cfg.afterRows) return false;
    this.fireFault('mid_prepare_big_tx', {
      txId,
      targetTxId: cfg.txId,
      targetRows: cfg.afterRows,
      cumulativePreparedRows: cumulative,
      batchRows: result.rows,
    });
    return true;
  }

  /**
   * The receive fault fires only after several pages are durable, proving the
   * offset bookkeeping survives a real restart rather than exiting on the very
   * first page (which would make recovery indistinguishable from a fresh run).
   */
  private maybeReceiveFault(): void {
    this.pagesPersisted += 1;
    if (this.opts.fault === 'after_receive_persist' && this.pagesPersisted === RECEIVE_FAULT_AFTER_PAGES) {
      this.fireFault('after_receive_persist', {
        pagesPersisted: this.pagesPersisted,
        receivedOffset: this.store.getReceivedOffset(),
      });
    }
  }

  private fireFault(point: FaultPoint, extra: Record<string, unknown>): void {
    this.faultConsumed = true;
    this.opts.log({ svc: 'sync', level: 'fatal', event: 'fault-exit', fault: point, ...extra });
    // Deferred so the current durable unit is fully flushed first.
    setImmediate(() => process.exit(42));
  }

  private async interruptibleSleep(ms: number): Promise<void> {
    await new Promise<void>((resolve) => {
      let settled = false;
      const finish = () => {
        if (settled) return; // a pause wake and the timer can never double-resolve
        settled = true;
        this.wakeWaiter = null;
        resolve();
      };
      const timer = setTimeout(finish, ms);
      this.wakeWaiter = () => {
        clearTimeout(timer);
        finish();
      };
    });
  }

  private async fetchPage(after: string): Promise<LogEnvelope> {
    const url = `${this.opts.sourceBaseUrl}/log?after=${encodeURIComponent(after)}&limit=${this.opts.pageLimit}`;
    const res = await fetch(url);
    if (res.status === 503) {
      const text = await res.text();
      throw new Error(`source 503: ${text}`);
    }
    if (!res.ok) throw new Error(`source HTTP ${res.status}`);
    return (await res.json()) as LogEnvelope;
  }
}
