/**
 * Shared integration-test harness: real child processes, real SQLite files,
 * real process restarts. Nothing here shares merge code with the sync engine.
 */

import { spawn, ChildProcess } from 'node:child_process';
import { createServer } from 'node:net';
import { mkdtemp, rm, readFile, mkdir } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';

export const ROOT = path.resolve(__dirname, '..');
export const TSX_ARGS = ['--import', 'tsx'];

export interface Harness {
  dir: string;
  sourcePort: number;
  syncPort: number;
  dbFile: string;
  source: ChildProcess;
  sync: ChildProcess | null;
  sourceLog: string[];
  syncLog: string[];
  teardown: () => Promise<void>;
}

export async function getFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.unref();
    srv.on('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      const addr = srv.address();
      if (addr && typeof addr === 'object') {
        const port = addr.port;
        srv.close(() => resolve(port));
      } else reject(new Error('no port'));
    });
  });
}

function pipeLines(child: ChildProcess, sink: string[]): void {
  child.stdout?.on('data', (b: Buffer) => sink.push(...b.toString().split('\n').filter(Boolean)));
  child.stderr?.on('data', (b: Buffer) => sink.push(...b.toString().split('\n').filter(Boolean)));
}

export async function startSource(opts: { overlap?: boolean; failEvery?: number } = {}): Promise<{
  source: ChildProcess;
  port: number;
  log: string[];
  stop: () => Promise<void>;
}> {
  const port = await getFreePort();
  const log: string[] = [];
  const source = spawn(process.execPath, [...TSX_ARGS, 'src/source/main.ts'], {
    cwd: ROOT,
    env: {
      ...process.env,
      SOURCE_PORT: String(port),
      SOURCE_OVERLAP: opts.overlap ? '1' : '',
      SOURCE_FAIL_EVERY: opts.failEvery ? String(opts.failEvery) : '',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  pipeLines(source, log);
  await waitForHttp(`http://127.0.0.1:${port}/health`, 10000);
  const stop = () =>
    new Promise<void>((resolve) => {
      source.once('exit', () => resolve());
      source.kill('SIGTERM');
    });
  return { source, port, log, stop };
}

export async function startSync(opts: {
  sourcePort: number;
  dbFile: string;
  fault?: string | null;
  autostart?: boolean;
  pageLimit?: number;
  prepareDelayMs?: number;
}): Promise<{ sync: ChildProcess; port: number; log: string[]; stop: () => Promise<void> }> {
  const port = await getFreePort();
  const log: string[] = [];
  const sync = spawn(process.execPath, [...TSX_ARGS, 'src/sync/main.ts'], {
    cwd: ROOT,
    env: {
      ...process.env,
      SYNC_PORT: String(port),
      SOURCE_URL: `http://127.0.0.1:${opts.sourcePort}`,
      SYNC_DB: opts.dbFile,
      SYNC_FAULT: opts.fault ?? '',
      SYNC_AUTOSTART: opts.autostart === false ? '' : '1',
      SYNC_PAGE_LIMIT: String(opts.pageLimit ?? 1000),
      SYNC_PREPARE_DELAY_MS: String(opts.prepareDelayMs ?? 0),
      SYNC_BACKOFF_MS: '25',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  pipeLines(sync, log);
  // A fault process may hard-exit (42) within the same millisecond it starts
  // listening, so the readiness poll must distinguish "crashed before we
  // polled" from "still starting": resolve on either an OK probe or an exit.
  await new Promise<void>((resolve, reject) => {
    let settled = false;
    const timer = setInterval(() => {
      if (settled) return;
      if (sync.exitCode !== null) {
        settled = true;
        clearInterval(timer);
        resolve(); // callers assert on the exit code via waitForExit
        return;
      }
      fetch(`http://127.0.0.1:${port}/status`)
        .then((r) => {
          if (!settled && r.ok) {
            settled = true;
            clearInterval(timer);
            resolve();
          }
        })
        .catch(() => undefined);
    }, 20);
    setTimeout(() => {
      if (!settled) {
        settled = true;
        clearInterval(timer);
        reject(new Error(`sync process never became ready on ${port}`));
      }
    }, 10000).unref();
  });
  const stop = () =>
    new Promise<void>((resolve) => {
      if (sync.exitCode !== null) return resolve();
      sync.once('exit', () => resolve());
      sync.kill('SIGTERM');
    });
  return { sync, port, log, stop };
}

export async function makeTempHarness(label: string): Promise<Harness> {
  const dir = await mkdtemp(path.join(os.tmpdir(), `wq-${label}-`));
  await mkdir(path.join(dir, 'data'), { recursive: true });
  const sourcePort = await getFreePort();
  const syncPort = await getFreePort();
  return {
    dir,
    sourcePort,
    syncPort,
    dbFile: path.join(dir, 'data', 'sync.db'),
    source: undefined as unknown as ChildProcess,
    sync: null,
    sourceLog: [],
    syncLog: [],
    teardown: async () => {
      await rm(dir, { recursive: true, force: true });
    },
  };
}

export async function httpJson<T = any>(url: string, init?: RequestInit): Promise<{ status: number; body: T }> {
  const res = await fetch(url, init);
  const text = await res.text();
  return { status: res.status, body: (text ? JSON.parse(text) : {}) as T };
}

export async function waitForHttp(url: string, timeoutMs = 15000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastErr: unknown;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(url);
      if (res.ok) return;
      lastErr = new Error(`HTTP ${res.status}`);
    } catch (err) {
      lastErr = err;
    }
    await sleep(30);
  }
  throw new Error(`timeout waiting for ${url}: ${String(lastErr)}`);
}

export function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/** Poll /status until predicate holds or timeout. */
export async function waitForStatus(
  port: number,
  predicate: (s: any) => boolean,
  timeoutMs = 30000,
): Promise<any> {
  const deadline = Date.now() + timeoutMs;
  let last: any;
  while (Date.now() < deadline) {
    const { body } = await httpJson(`http://127.0.0.1:${port}/status`);
    last = body;
    if (predicate(body)) return body;
    await sleep(15);
  }
  throw new Error(`status predicate not met; last=${JSON.stringify(last)}`);
}

/** Page through the entire readings list (keyset pagination). */
export async function readAllReadings(port: number, limit = 1000): Promise<{ schemaVersion: number; items: any[] }> {
  let after: string | null = null;
  const items: any[] = [];
  let schemaVersion = 1;
  for (;;) {
    const q = new URL(`http://127.0.0.1:${port}/readings`);
    q.searchParams.set('limit', String(limit));
    if (after) q.searchParams.set('after', after);
    const { body } = await httpJson<any>(q.toString());
    schemaVersion = body.schemaVersion;
    items.push(...body.items);
    if (!body.nextRecordId) break;
    after = body.nextRecordId;
  }
  return { schemaVersion, items };
}

export function parseStructuredLogs(lines: string[]): any[] {
  const out: any[] = [];
  for (const line of lines) {
    try {
      out.push(JSON.parse(line));
    } catch {
      /* ignore non-JSON lines (warnings etc.) */
    }
  }
  return out;
}

export async function readText(file: string): Promise<string> {
  return readFile(file, 'utf8');
}
