/**
 * Sync service HTTP API:
 *   POST /sync/start   - start (idempotent; never starts a second loop)
 *   POST /sync/pause   - stop after the current durable batch finishes
 *   GET  /status       - durable positions, schema, tx counters, retries, error
 *   GET  /readings     - keyset pagination ordered by recordId (?limit&after)
 *   GET  /readings/:id - single record, 404 once deleted / not yet visible
 *   GET  /diagnostics  - structured pipeline diagnostics (staging visibility)
 */

import { createServer, IncomingMessage, ServerResponse } from 'node:http';
import { URL } from 'node:url';
import { SyncStore } from './store';
import { SyncEngine } from './engine';
import type { MidPrepareFaultConfig } from './engine';
import type { FaultPoint, ListResponse, SyncStatus } from '../shared/protocol';

export interface SyncServerOptions {
  dbFile: string;
  sourceBaseUrl: string;
  port: number;
  pageLimit: number;
  fault: FaultPoint | null;
  midPrepare: MidPrepareFaultConfig | null;
  log?: (line: Record<string, unknown>) => void;
}

export function createSyncServer(options: SyncServerOptions) {
  const store = new SyncStore(options.dbFile);
  const engine = new SyncEngine(store, {
    sourceBaseUrl: options.sourceBaseUrl,
    pageLimit: options.pageLimit,
    baseBackoffMs: Number(process.env.SYNC_BACKOFF_MS ?? 100),
    maxBackoffMs: Number(process.env.SYNC_MAX_BACKOFF_MS ?? 2000),
    prepareBatchDelayMs: Number(process.env.SYNC_PREPARE_DELAY_MS ?? 0),
    fault: options.fault,
    midPrepare: options.midPrepare,
    log: options.log,
  });

  const json = (res: ServerResponse, status: number, body: unknown) => {
    res.writeHead(status, { 'content-type': 'application/json' }).end(JSON.stringify(body));
  };

  const server = createServer((req: IncomingMessage, res: ServerResponse) => {
    if (!req.url || !req.method) {
      json(res, 400, { error: 'bad request' });
      return;
    }
    const url = new URL(req.url, 'http://localhost');
    const path = url.pathname;

    if (req.method === 'POST' && path === '/sync/start') {
      engine
        .start()
        .then((started) => json(res, 200, { started, running: engine.isRunning(), receivedOffset: store.getReceivedOffset() }))
        .catch((err) => json(res, 500, { error: String(err) }));
      return;
    }
    if (req.method === 'POST' && path === '/sync/pause') {
      engine
        .pause()
        .then(() => {
          // Snapshot every durable position after the loop has actually
          // stopped, so callers can assert stability directly from the body.
          const status: SyncStatus = store.getStatus(engine.isRunning());
          json(res, 200, {
            paused: true,
            running: status.running,
            receivedOffset: status.receivedOffset,
            openTransactions: status.openTransactions,
            preparedTransactions: status.preparedTransactions,
            publishedTransactions: status.publishedTransactions,
          });
        })
        .catch((err) => json(res, 500, { error: String(err) }));
      return;
    }
    if (req.method === 'GET' && path === '/status') {
      const status: SyncStatus = store.getStatus(engine.isRunning());
      json(res, 200, status);
      return;
    }
    if (req.method === 'GET' && path === '/diagnostics') {
      json(res, 200, store.diagnostics());
      return;
    }
    if (req.method === 'GET' && path === '/readings') {
      const limit = Math.min(Math.max(Number(url.searchParams.get('limit') ?? 100) || 100, 1), 500);
      const after = url.searchParams.get('after');
      const { items, nextRecordId } = store.listReadings(limit, after);
      const body: ListResponse = { schemaVersion: store.getSchemaVersion(), limit, nextRecordId, items };
      json(res, 200, body);
      return;
    }
    const single = path.match(/^\/readings\/([^/]+)$/);
    if (req.method === 'GET' && single) {
      const recordId = decodeURIComponent(single[1]);
      const reading = store.getReading(recordId);
      if (!reading) {
        json(res, 404, { error: 'record not visible', recordId });
        return;
      }
      json(res, 200, { schemaVersion: store.getSchemaVersion(), item: reading });
      return;
    }
    json(res, 404, { error: 'not found' });
  });

  return {
    server,
    store,
    engine,
    listen: () =>
      new Promise<void>((resolve) => {
        server.listen(options.port, () => {
          (options.log ?? console.log.bind(console))(
            JSON.stringify({ svc: 'sync', event: 'listening', port: options.port, db: options.dbFile, fault: options.fault }),
          );
          resolve();
        });
      }),
    close: async () => {
      if (engine.isRunning()) await engine.pause();
      await new Promise<void>((r) => server.close(() => r()));
      store.close();
    },
  };
}
