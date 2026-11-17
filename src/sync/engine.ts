/**
 * Synchronization engine: pull loop, retry/backoff, prepare/publish pipeline,
 * one-shot fault injection and structured diagnostics.
 *
 * Streaming guarantee: the source is read one page (at most `pageLimit`
 * events) at a time and prepare batches are capped at MAX_PREPARE_ROWS rows.
 * Neither the whole log nor the 20k-row transaction is ever materialized in
 * memory at once.
 */

import { SyncStore, MAX_PREPARE_ROWS } from './store';
import type { DeliveredEvent, FaultPoint, LogEnvelope } from '../shared/protocol';
import { LARGE_TX_PREPARE_BATCH } from '../fixtures/fixtures';

/** Persist this many pages before the after_receive_persist fault exits. */
const RECEIVE_FAULT_AFTER_PAGES = 2;

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
  /** structured log sink (injectable for tests). */
  log?: (line: Record<string, unknown>) => void;
}

export class SyncEngine {
  private store: SyncStore;
  private opts: Required<Omit<EngineOptions, 'fault'>> & { fault: FaultPoint | null };
  private running = false;
  private paused = true;
  private loopPromise: Promise<void> | null = null;
  private wakeWaiter: (() => void) | null = null;
  private faultConsumed = false;

  constructor(store: SyncStore, options: EngineOptions) {
    this.store = store;
    this.opts = {
      pageLimit: options.pageLimit,
      baseBackoffMs: options.baseBackoffMs,
      maxBackoffMs: options.maxBackoffMs,
      prepareBatchDelayMs: options.prepareBatchDelayMs,
      sourceBaseUrl: options.sourceBaseUrl,
      fault: options.fault ?? null,
      log: options.log ?? ((line) => console.log(JSON.stringify(line))),
    };
  }

  isRunning(): boolean {
    return this.running;
  }
  isPaused(): boolean {
    return this.paused;
  }

  start(): boolean {
    if (this.running) return false; // duplicate start never creates a second loop
    this.running = true;
    this.paused = false;
    this.loopPromise = this.loop().catch((err) => {
      this.opts.log({ svc: 'sync', level: 'error', event: 'loop-terminated', error: String(err) });
    });
    return true;
  }

  /** Stop pulling after the current durable batch / prepare / publish step. */
  async pause(): Promise<void> {
    this.paused = true;
    if (this.wakeWaiter) this.wakeWaiter(); // abort any backoff sleep immediately
    if (this.loopPromise) await this.loopPromise;
    this.running = false;
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

      const entries = page.events.map((event) => ({
        // Overlap pages repeat events whose offset is <= the prior tail;
        // the simulator annotates each event with its real transport offset.
        offset: (event as DeliveredEvent)._offset,
        event,
      }));
      if (entries.length > 0) {
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

      // Schema declarations may arrive late; apply before evaluating candidates.
      const applied = this.store.applyDueSchemaEvents();
      for (const a of applied) this.opts.log({ svc: 'sync', event: 'schema-applied', schemaVersion: a.event.schemaVersion });

      // Prepare + publish whatever is currently completable, in small units so
      // reads stay responsive and memory stays bounded.
      await this.drainPipeline();

      if (!page.hasMore && entries.length === 0) {
        // Caught up. Sleep briefly but remain interruptible by pause.
        await this.interruptibleSleep(50);
      }
    }
    this.opts.log({ svc: 'sync', event: 'loop-paused', receivedOffset: this.store.getReceivedOffset() });
  }

  private async drainPipeline(): Promise<void> {
    // Prepare in <=200-row batches while eligible work exists. A large tx
    // spans many batches; when its current batch is exhausted we keep looping
    // so other fully-received transactions can be prepared/published instead
    // of stalling behind the in-progress one.
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const status = this.store.getStatus(this.running);
      if (status.openTransactions === 0) break;
      const faultAfter =
        this.opts.fault === 'mid_prepare_big_tx' && !this.faultConsumed ? LARGE_TX_PREPARE_BATCH : undefined;
      const result = this.store.prepareBatch(faultAfter);
      if (!result.txId) break;
      this.opts.log({
        svc: 'sync',
        event: result.finished ? 'tx-prepared' : 'prepare-batch',
        txId: result.txId,
        rows: result.rows,
        batchCap: MAX_PREPARE_ROWS,
      });
      if (this.opts.fault === 'mid_prepare_big_tx' && faultAfter !== undefined && result.rows >= faultAfter) {
        this.fireFault('mid_prepare_big_tx', { txId: result.txId });
        return;
      }
      // Yield between batches: keeps the read API responsive and, when
      // configured, widens the prepare window for test probes.
      if (this.opts.prepareBatchDelayMs > 0) await this.interruptibleSleep(this.opts.prepareBatchDelayMs);
      else await new Promise((r) => setImmediate(r));
      // If this batch did not finish a large tx, try the next candidate; when
      // no other candidate exists prepareBatch returns null and we exit the
      // loop until more rows are received on subsequent pages.
      if (!result.finished) continue;
    }

    // Publish every fully-prepared transaction in a short commit per tx.
    if (this.store.hasPrepared()) {
      if (this.opts.fault === 'before_publish_commit' && !this.faultConsumed) {
        // Fire as soon as the first transaction is fully staged, so the
        // restart must reconcile prepared-but-unpublished state itself.
        this.fireFault('before_publish_commit', { prepared: this.store.preparedTxIds().length });
        return;
      }
      const ids = this.store.publishReady();
      for (const txId of ids) this.opts.log({ svc: 'sync', event: 'tx-published', txId, via: 'short-commit' });
      if (this.opts.fault === 'after_publish_commit' && !this.faultConsumed) {
        // Fire after the first publish commit; the restart proves the already
        // committed publish is not duplicated while remaining txs still flow.
        this.fireFault('after_publish_commit', { published: ids.length });
        return;
      }
    }
  }

  private pagesPersisted = 0;

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
      const timer = setTimeout(() => {
        this.wakeWaiter = null;
        resolve();
      }, ms);
      this.wakeWaiter = () => {
        clearTimeout(timer);
        this.wakeWaiter = null;
        resolve();
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
