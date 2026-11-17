/**
 * Sync service process entry point.
 *
 * Env:
 *   SYNC_PORT       (default 8081)
 *   SOURCE_URL      (default http://127.0.0.1:8080)
 *   SYNC_DB         SQLite file path (default data/sync.db)
 *   SYNC_PAGE_LIMIT events per fetched page (default 1000)
 *   SYNC_FAULT      one of the FaultPoint values; one-shot, then process exits
 *   SYNC_AUTOSTART  1 to start pulling immediately (used by the test harness)
 *   SYNC_MID_PREPARE_TX   target txId for mid_prepare_big_tx (default TX-0010)
 *   SYNC_MID_PREPARE_ROWS cumulative staged rows that trigger it (default 10000)
 */

import { createSyncServer } from './server';
import type { MidPrepareFaultConfig } from './engine';
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

/**
 * Parse the configurable mid-prepare fault target. Defaults are the built-in
 * 20k-row transaction and its 10,000th prepared row. Invalid values fall back
 * to the defaults instead of crashing the whole service.
 */
function resolveMidPrepare(): MidPrepareFaultConfig {
  const txId = process.env.SYNC_MID_PREPARE_TX?.trim() || 'TX-0010';
  const rawRows = Number(process.env.SYNC_MID_PREPARE_ROWS ?? 10000);
  const afterRows = Number.isInteger(rawRows) && rawRows > 0 ? rawRows : 10000;
  return { txId, afterRows };
}

const app = createSyncServer({
  dbFile: process.env.SYNC_DB ?? 'data/sync.db',
  sourceBaseUrl: process.env.SOURCE_URL ?? 'http://127.0.0.1:8080',
  port: Number(process.env.SYNC_PORT ?? 8081),
  pageLimit: Number(process.env.SYNC_PAGE_LIMIT ?? 1000),
  fault,
  midPrepare: resolveMidPrepare(),
});

async function main(): Promise<void> {
  await app.listen();
  if (process.env.SYNC_AUTOSTART === '1') {
    void app.engine.start();
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
