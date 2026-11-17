/**
 * Sync service process entry point.
 *
 * Env:
 *   SYNC_PORT       (default 8081)
 *   SOURCE_URL      (default http://127.0.0.1:8080)
 *   SYNC_DB         SQLite file path (default data/sync.db)
 *   SYNC_PAGE_LIMIT events per fetched page (default 250)
 *   SYNC_FAULT      one of the FaultPoint values; one-shot, then process exits
 *   SYNC_AUTOSTART  1 to start pulling immediately (used by the test harness)
 */

import { createSyncServer } from './server';
import type { FaultPoint } from '../shared/protocol';

const validFaults = new Set<FaultPoint>([
  'after_receive_persist',
  'mid_prepare_big_tx',
  'before_publish_commit',
  'after_publish_commit',
]);

const fault = process.env.SYNC_FAULT && validFaults.has(process.env.SYNC_FAULT as FaultPoint)
  ? (process.env.SYNC_FAULT as FaultPoint)
  : null;

const app = createSyncServer({
  dbFile: process.env.SYNC_DB ?? 'data/sync.db',
  sourceBaseUrl: process.env.SOURCE_URL ?? 'http://127.0.0.1:8080',
  port: Number(process.env.SYNC_PORT ?? 8081),
  pageLimit: Number(process.env.SYNC_PAGE_LIMIT ?? 1000),
  fault,
});

async function main(): Promise<void> {
  await app.listen();
  if (process.env.SYNC_AUTOSTART === '1') {
    app.engine.start();
  }
  const shutdown = async (signal: string) => {
    console.log(JSON.stringify({ svc: 'sync', event: 'shutdown', signal }));
    await app.close();
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
}

main().catch((err) => {
  console.error(JSON.stringify({ svc: 'sync', level: 'fatal', error: String(err) }));
  process.exit(1);
});
