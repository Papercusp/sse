/**
 * resilientPostStream — reconnect-safe SSE over a POST request.
 *
 * `EventSource` (and so `createResilientEventSource`) can only GET, but many
 * streams need a request body (a chat turn, a query). This is the POST-capable
 * sibling: it POSTs, reads the response body with {@link parseSseStream}, and —
 * if the connection drops before a terminal event — RESUMES the same logical
 * stream with `Last-Event-ID` + a caller-built resume body, under bounded
 * exponential backoff. The server is expected to keep the turn alive and replay
 * missed events on resume (the scout reconnect-safe protocol).
 *
 * Yields each JSON-decoded event payload; stops after the first terminal event.
 * Non-JSON `data`, empty `data`, and `skipEvents` (default: heartbeat) are
 * dropped. Generalizes the hand-rolled loop that lived in RecommenderChat.
 */

import { parseSseStream } from './parse-stream';

export interface ResilientPostResume {
  /** Resumable turn id from the response header (see `turnIdHeader`), or null. */
  turnId: string | null;
  /** Highest SSE event id seen so far (sent as Last-Event-ID on resume). */
  lastEventId: number;
}

export interface ResilientPostStreamOptions {
  url: string;
  /** Build the request body. `resume` is null on the first attempt, else the
   *  cursor for a reconnect — return the resume payload (e.g. `{ turnId, lastEventId }`). */
  buildBody: (resume: ResilientPostResume | null) => unknown;
  /** True when a decoded payload is terminal (ends the stream — e.g. `done`/`error`). */
  isTerminal: (data: unknown) => boolean;
  /** Response header carrying the resumable turn id (e.g. `X-Scout-Turn-Id`). */
  turnIdHeader?: string;
  /** Extra request headers (Content-Type: application/json is always set). */
  headers?: Record<string, string>;
  /** Max resume attempts after the first stream drops. Default 5. */
  maxResumeAttempts?: number;
  /** Backoff ms = min(base * 2^(attempt-1), cap). Defaults: base 400, cap 4000. */
  backoffBaseMs?: number;
  backoffCapMs?: number;
  /** SSE event names to drop (not yielded). Default `['heartbeat']`. */
  skipEvents?: string[];
  /** Abort the stream (and skip pending resumes). */
  signal?: AbortSignal;
  /** Injectable for tests; defaults to global fetch. */
  fetchImpl?: typeof fetch;
  /** Injectable for tests; defaults to setTimeout-based delay. */
  delayImpl?: (ms: number) => Promise<void>;
}

export interface ResilientStreamEvent<T = unknown> {
  /** JSON-decoded `data`. */
  data: T;
  id?: number | string;
  event?: string;
  /** True for the terminal event that ends the stream. */
  terminal: boolean;
}

const defaultDelay = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export async function* resilientPostStream<T = unknown>(
  opts: ResilientPostStreamOptions,
): AsyncGenerator<ResilientStreamEvent<T>, void, unknown> {
  const {
    url,
    buildBody,
    isTerminal,
    turnIdHeader,
    headers = {},
    maxResumeAttempts = 5,
    backoffBaseMs = 400,
    backoffCapMs = 4000,
    skipEvents = ['heartbeat'],
    signal,
    fetchImpl = fetch,
    delayImpl = defaultDelay,
  } = opts;

  let turnId: string | null = null;
  let lastEventId = 0;
  let completed = false;

  async function* openOnce(resume: boolean): AsyncGenerator<ResilientStreamEvent<T>, void, unknown> {
    const res = await fetchImpl(url, {
      method: 'POST',
      signal,
      headers: {
        'Content-Type': 'application/json',
        ...headers,
        ...(resume && lastEventId ? { 'Last-Event-ID': String(lastEventId) } : {}),
      },
      body: JSON.stringify(buildBody(resume ? { turnId, lastEventId } : null)),
    });
    if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);
    if (turnIdHeader) turnId = res.headers.get(turnIdHeader) || turnId;

    try {
      for await (const ev of parseSseStream(res.body)) {
        if (ev.event && skipEvents.includes(ev.event)) continue;
        if (ev.id != null && ev.id !== '') lastEventId = Math.max(lastEventId, Number(ev.id));
        if (!ev.data) continue;
        let data: T;
        try { data = JSON.parse(ev.data) as T; } catch { continue; }
        const terminal = isTerminal(data);
        yield { data, id: ev.id, event: ev.event, terminal };
        // Terminal event ends the turn. The reconnect-safe server keeps the SSE
        // connection OPEN after it (so a dropped client can resume), so stop
        // explicitly — else this read loop awaits the next event forever.
        if (terminal) { completed = true; return; }
      }
    } finally {
      // Stop reading ⇒ close the still-open connection, else each finished turn
      // leaks an open socket and the per-host pool starves later requests.
      try { await res.body?.cancel(); } catch { /* already closing */ }
    }
  }

  yield* openOnce(false);

  // Stream ended without a terminal event ⇒ the connection dropped. Resume the
  // same turn from where we left off, with bounded backoff.
  for (let attempt = 1; !completed && turnId && attempt <= maxResumeAttempts; attempt++) {
    if (signal?.aborted) return;
    await delayImpl(Math.min(backoffBaseMs * 2 ** (attempt - 1), backoffCapMs));
    try {
      yield* openOnce(true);
    } catch {
      /* keep retrying until the attempt budget is spent */
    }
  }
}
