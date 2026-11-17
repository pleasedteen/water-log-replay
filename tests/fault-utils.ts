/**
 * Shared helpers for the fault-injection / pause integration test files.
 *
 * These tests run real child sync processes against real SQLite files and
 * reopen those files independently to assert durable watermarks. Pause tests
 * never rely on a fixed startup sleep: they wait on durable progress visible
 * through /diagnostics or /status before sending pause.
 */

import assert from 'node:assert/strict';
import { mkdtemp, rm, readdir } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { startSync, waitForStatus, readAllReadings, httpJson } from './helpers';
import { referenceReplay } from './reference-replayer';
import { TX } from '../src/fixtures/fixtures';
import type { ChildProcess } from 'node:child_process';
import type { SyncStatus } from '../src/shared/protocol';

const tempDirs: string[] = [];
export async function newDir(label: string): Promise<string> {
  const d = await mkdtemp(path.join(os.tmpdir(), `wq-${label}-`));
  tempDirs.push(d);
  return d;
}
export async function cleanupDirs(): Promise<void> {
  for (const d of tempDirs) await rm(d, { recursive: true, force: true });
}

export async function waitForExit(child: ChildProcess, timeoutMs = 30000): Promise<number | null> {
  if (child.exitCode !== null) return child.exitCode;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('process did not exit on fault')), timeoutMs);
    child.once('exit', (code) => {
      clearTimeout(timer);
      resolve(code);
    });
  });
}

/**
 * Reopen the exact SQLite file independently of the sync implementation and
 * read the durable watermarks straight from disk. The sync process is already
 * stopped, so this proves progress survived a real process boundary.
 */
export function readOnDiskState(dbFile: string) {
  const db = new DatabaseSync(dbFile);
  try {
    db.exec('pragma wal_checkpoint(TRUNCATE)');
    const meta = db.prepare('select received_offset, schema_version, retry_count from meta where id=1').get() as {
      received_offset: string;
      schema_version: number;
      retry_count: number;
    };
    const tx = db
      .prepare('select tx_id, status, expected_count, received_count, prepared_count from tx_state where tx_id=?')
      .get(TX.T10.id) as {
      tx_id: string;
      status: string;
      expected_count: number;
      received_count: number;
      prepared_count: number;
    };
    const staged = db.prepare('select count(*) c from tx_rows where tx_id=? and staged=1').get(TX.T10.id) as { c: number };
    const published = db.prepare('select count(*) c from published_txns').get() as { c: number };
    const publishedBig = db
      .prepare(
        `select count(*) c from record_versions rv join published_txns p on p.tx_version=rv.tx_version
         where rv.record_id like 'BIG-%'`,
      )
      .get() as { c: number };
    return { meta, tx, stagedRows: staged.c, publishedTxns: published.c, publishedBigRows: publishedBig.c };
  } finally {
    db.close();
  }
}

export async function assertConvergedWithReplay(port: number): Promise<{ status: SyncStatus; items: any[] }> {
  const status = await waitForStatus(
    port,
    (s: SyncStatus) => s.publishedTransactions === 9 && s.openTransactions === 1 && s.schemaVersion === 2,
    30000,
  );
  const { items, schemaVersion } = await readAllReadings(port);
  const expected = referenceReplay();
  assert.equal(schemaVersion, expected.schemaVersion);
  assert.equal(items.length, expected.records.size);
  const got = new Map(items.map((i) => [i.recordId, i]));
  for (const [id, want] of expected.records) {
    const g = got.get(id);
    assert.ok(g, `record ${id} present after convergence`);
    assert.equal(g.nitrateMgL, want.nitrateMgL);
    assert.equal(g.methodCode, want.methodCode);
  }
  const r6 = await httpJson(`http://127.0.0.1:${port}/readings/R-0006`);
  assert.equal(r6.status, 404, 'never-completed T7 stays invisible');
  return { status, items };
}

/** Lightweight convergence wait: status only, no full list read / replay compare. */
export async function waitConverged(port: number, timeoutMs = 30000): Promise<SyncStatus> {
  return waitForStatus(
    port,
    (s: SyncStatus) => s.publishedTransactions === 9 && s.openTransactions === 1 && s.schemaVersion === 2,
    timeoutMs,
  );
}

export { readdir, startSync };
