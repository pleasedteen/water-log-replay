/**
 * Local source simulator.
 *
 * Serves GET /log?after=<decimal>&limit=<n> from an in-memory, deterministic
 * log built from the checked-in fixtures. No network access beyond localhost.
 *
 * Extra behaviours (all deterministic, configured via env vars):
 *  SOURCE_OVERLAP=1        each page after the first also re-sends up to
 *                          OVERLAP_SIZE events whose offset is <= the previous
 *                          page's tail (the sync side dedupes on offset PK)
 *  SOURCE_FAIL_EVERY=K     every K-th *non-overlapping* page request answers
 *                          HTTP 503 once (used to prove backoff/retry)
 *  SOURCE_PORT             listen port (default 8080)
 */

import { createServer, IncomingMessage, ServerResponse } from 'node:http';
import { URL } from 'node:url';
import { buildFixture, FIXTURE_BASE_OFFSET } from '../fixtures/fixtures';
import type { LogEnvelope, LogEvent } from '../shared/protocol';

interface LogEntry {
  offset: bigint;
  event: LogEvent;
}

function buildLog(): LogEntry[] {
  const fixture = buildFixture();
  return fixture.events.map((event, i) => ({ offset: FIXTURE_BASE_OFFSET + BigInt(i), event }));
}

const OVERLAP_SIZE = 2;
const OVERLAP_EVERY = 5;

/**
 * Attach the transport position. `__offset` is non-enumerable so it does not
 * appear in the JSON payload; a parallel, enumerable `_offset` field IS sent
 * on the wire and used by the sync engine to persist each event at its exact
 * source position even when pages overlap.
 */
function withOffset(event: LogEvent, offset: bigint): LogEvent {
  const out: LogEvent & { _offset: string } = { ...event, _offset: offset.toString() };
  Object.defineProperty(out, '__offset', { value: offset.toString(), enumerable: false });
  return out;
}

export function createSourceServer(opts: { overlap: boolean; failEvery: number }) {
  const log = buildLog();
  const lastOffset = log.length === 0 ? 0n : log[log.length - 1].offset;
  let pageRequests = 0; // counts distinct forward (non-overlap) page requests

  function handlePage(after: bigint, limit: number): { status: number; body?: LogEnvelope } {
    // Overlap rows do not constitute a new "forward" request; detect them by
    // whether `after` has previously been served.
    const isOverlapRepeat = seenAfter.has(after.toString());
    if (!isOverlapRepeat) {
      seenAfter.add(after.toString());
      pageRequests += 1;
      if (opts.failEvery > 0 && pageRequests % opts.failEvery === 0) {
        return { status: 503 };
      }
    }

    const startIdx = log.findIndex((e) => e.offset > after);
    const begin = startIdx === -1 ? log.length : startIdx;
    const forward = log.slice(begin, begin + limit);
    let events: LogEvent[] = forward.map((e) => withOffset(e.event, e.offset));
    let tailOffset = after;
    if (forward.length > 0) tailOffset = forward[forward.length - 1].offset;

    // Deterministic overlap: every OVERLAP_EVERY-th forward page repeats up to
    // OVERLAP_SIZE already-seen entries (with offsets <= the previous tail).
    // Their original offsets are annotated so the sync side dedupes on the
    // offset primary key. Periodic overlap keeps large logs bounded in page
    // count while still exercising re-delivery throughout the stream.
    const overlapThisPage = opts.overlap && pageRequests % OVERLAP_EVERY === 0;
    if (overlapThisPage && begin > 0 && forward.length > 0) {
      const overlapStart = Math.max(0, begin - OVERLAP_SIZE);
      const overlapEntries = log
        .slice(overlapStart, begin)
        .map((e) => withOffset(e.event, e.offset));
      events = [...overlapEntries, ...events];
    }

    const hasMore = begin + limit < log.length;
    const nextOffset = forward.length === 0 ? after.toString() : tailOffset.toString();
    return {
      status: 200,
      body: { events, nextOffset, hasMore: hasMore || (forward.length === 0 && after < lastOffset) },
    };
  }

  const seenAfter = new Set<string>();

  const server = createServer((req: IncomingMessage, res: ServerResponse) => {
    if (req.method !== 'GET' || !req.url) {
      res.writeHead(405).end('method not allowed');
      return;
    }
    const url = new URL(req.url, 'http://localhost');
    if (url.pathname === '/health') {
      res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ ok: true, logLength: log.length }));
      return;
    }
    if (url.pathname !== '/log') {
      res.writeHead(404).end('not found');
      return;
    }
    const afterRaw = url.searchParams.get('after') ?? '0';
    const limitRaw = Number(url.searchParams.get('limit') ?? '100');
    let after: bigint;
    try {
      if (!/^\d+$/.test(afterRaw)) throw new Error('bad offset');
      after = BigInt(afterRaw);
    } catch {
      res.writeHead(400).end('after must be a decimal string');
      return;
    }
    if (!Number.isInteger(limitRaw) || limitRaw <= 0 || limitRaw > 1000) {
      res.writeHead(400).end('limit must be an integer in (0,1000]');
      return;
    }
    const result = handlePage(after, limitRaw);
    if (result.status === 503) {
      res.writeHead(503, { 'content-type': 'application/json', 'retry-after': '0' }).end(
        JSON.stringify({ error: 'temporarily unavailable (injected fault)' }),
      );
      return;
    }
    res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(result.body));
  });

  return { server, logSize: log.length };
}

if (require.main === module) {
  const port = Number(process.env.SOURCE_PORT ?? 8080);
  const overlap = process.env.SOURCE_OVERLAP === '1';
  const failEvery = Number(process.env.SOURCE_FAIL_EVERY ?? 0);
  const { server, logSize } = createSourceServer({ overlap, failEvery });
  server.listen(port, () => {
    // structured diagnostic line
    console.log(JSON.stringify({ svc: 'source', event: 'listening', port, logSize, overlap, failEvery }));
  });
}
