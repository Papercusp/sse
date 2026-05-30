/**
 * sseResponse — build a spec-compliant SSE Response.
 *
 * Encapsulates the pattern every SSE route in the operator was implementing
 * by hand: ReadableStream + encoder + heartbeat timer + abort wiring +
 * standard headers. Adds:
 *   - Auto-monotonic event ids (so Last-Event-ID reconnect resumes correctly)
 *   - Initial heartbeat on open
 *   - Late-join terminal short-circuit (no client-hangs after channel is done)
 *   - Ring-buffer replay filtered by Last-Event-ID
 *   - Single onClose path for all close triggers (cancel, signal abort, done, close)
 *
 * Wire format is fixed (STABILITY CONTRACT — see README). The lifecycle is
 * deterministic; see comments at sseResponse() body for the open-time order.
 */

import { encodeFrame, heartbeatFrame } from '../wire/format';
import { createIdAllocator } from '../wire/ids';

export interface SseSink<TEvents extends Record<string, unknown> = Record<string, unknown>> {
  /**
   * Emit a named event. Auto-monotonic id unless explicit `id` provided.
   * Data is JSON-stringified. No-op after close.
   */
  event<K extends keyof TEvents & string>(
    name: K,
    data: TEvents[K],
    opts?: { id?: number | string }
  ): void;

  /**
   * Emit a named event with pre-formatted string data (NO JSON.stringify wrap).
   * Use for line-streaming or raw-text protocols (e.g. LLM token deltas).
   * For most cases prefer `event()` — this is the escape hatch.
   */
  eventRaw(
    name: string,
    data: string,
    opts?: { id?: number | string }
  ): void;

  /** Comment line. Use sparingly — prefer `heartbeat()` for keepalive. */
  comment(text: string): void;

  /**
   * Send a heartbeat event NOW (in addition to the timer).
   * Always `event: heartbeat\ndata: {"tsMs":<now>}`. No id.
   */
  heartbeat(): void;

  /**
   * Emit terminal `done` event with optional payload, then close.
   * Idempotent.
   */
  done(payload?: unknown): void;

  /** Force-close without emitting `done`. For error paths. Idempotent. */
  close(): void;

  /**
   * Register cleanup; runs exactly once on the first of:
   *   signal abort | ReadableStream cancel() | sink.done() | sink.close()
   */
  onClose(fn: () => void): void;

  /** True after first of done()/close()/cancel(). */
  readonly closed: boolean;
}

export interface SseResponseOptions<TEvents extends Record<string, unknown>> {
  /** Required — typically `req.signal`. Triggers cleanup on client disconnect. */
  signal: AbortSignal;

  /** Parsed Last-Event-ID. null = no resume. */
  lastEventId?: number | null;

  /**
   * Invoked once after replay completes. Use sink to publish.
   * If this rejects, sink auto-emits `event: error` with the message, then closes.
   */
  setup: (sink: SseSink<TEvents>) => void | Promise<void>;

  /** Heartbeat interval ms; 0 disables. Default 15_000. */
  heartbeatMs?: number;

  /** Send a heartbeat at open to confirm liveness. Default true. */
  initialHeartbeat?: boolean;

  /**
   * Optional ring-buffer replay. Called once at open AFTER lastEventId is
   * resolved; lib filters items whose id <= lastEventId.
   */
  replay?: () => Iterable<{ name: keyof TEvents & string; data: TEvents[keyof TEvents]; id: number }>;

  /**
   * If the source is already terminal at open, emit `done` with the returned
   * payload and close immediately. Client doesn't hang waiting for events
   * that will never come.
   */
  terminalShortCircuit?: () => null | { reason: string; payload?: unknown };

  /** Extra response headers (merged over defaults). */
  headers?: HeadersInit;
}

const DEFAULT_HEADERS: HeadersInit = {
  'Content-Type': 'text/event-stream',
  'Cache-Control': 'no-cache, no-transform',
  'Connection': 'keep-alive',
  'X-Accel-Buffering': 'no',
};

/**
 * Bridge an async iterable into the sink, emitting one event per item.
 *
 * Handles two cleanup paths producers always need but easily forget:
 *   - When sink closes (client disconnect, done, error): we call `.return()`
 *     on the underlying iterator so the producing channel gets unsubscribed.
 *   - When the source iteration ends naturally: we leave the sink open
 *     (it's the caller's choice whether to also call sink.done()).
 *
 * Usage:
 *   setup: (sink) => bridgeChannel(ch.subscribe(), sink, ({ id, event }) =>
 *     ({ name: 'update', data: event, opts: { id } })
 *   ),
 */
export async function bridgeChannel<TItem, TEvents extends Record<string, unknown>>(
  source: AsyncIterable<TItem>,
  sink: SseSink<TEvents>,
  map: (item: TItem) => {
    name: keyof TEvents & string;
    // Mirror the `name` key constraint so `sink.event<K>(name, data)` (whose
    // K extends `keyof TEvents & string`) accepts `data` — `TEvents[keyof TEvents]`
    // and `TEvents[keyof TEvents & string]` are distinct index types to tsc.
    data: TEvents[keyof TEvents & string];
    opts?: { id?: number | string };
  } | null,
): Promise<void> {
  const iter = source[Symbol.asyncIterator]();
  sink.onClose(() => { void iter.return?.(undefined); });
  while (!sink.closed) {
    const { value, done } = await iter.next();
    if (done) return;
    const out = map(value);
    if (out) sink.event(out.name, out.data, out.opts);
  }
}

/** Parse the Last-Event-ID header as a positive integer; null if absent or invalid. */
export function parseLastEventId(req: Request): number | null {
  const raw = req.headers.get('Last-Event-ID');
  if (!raw) return null;
  const n = Number(raw);
  return Number.isFinite(n) && n >= 0 ? n : null;
}

export function sseResponse<TEvents extends Record<string, unknown> = Record<string, unknown>>(
  opts: SseResponseOptions<TEvents>
): Response {
  const ids = createIdAllocator(opts.lastEventId ?? 0);
  const heartbeatMs = opts.heartbeatMs ?? 15_000;
  const initialHeartbeat = opts.initialHeartbeat ?? true;
  const sinceId = opts.lastEventId ?? 0;

  let closed = false;
  let controllerRef: ReadableStreamDefaultController<Uint8Array> | null = null;
  let heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  const closeHandlers: Array<() => void> = [];

  const enqueue = (bytes: Uint8Array): void => {
    if (closed || !controllerRef) return;
    try {
      controllerRef.enqueue(bytes);
    } catch {
      // Controller already closed by upstream; mark closed and run cleanup.
      runClose();
    }
  };

  const runClose = (): void => {
    if (closed) return;
    closed = true;
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
    // Run handlers in registration order; swallow errors so one bad handler
    // doesn't block the others.
    for (const fn of closeHandlers) {
      try { fn(); } catch { /* ignore */ }
    }
    try { controllerRef?.close(); } catch { /* already closed */ }
    controllerRef = null;
  };

  const sink: SseSink<TEvents> = {
    event(name, data, eventOpts) {
      if (closed) return;
      const id = eventOpts?.id ?? ids.next();
      enqueue(encodeFrame({ id, event: name, data: JSON.stringify(data) }));
    },
    eventRaw(name, data, eventOpts) {
      if (closed) return;
      const id = eventOpts?.id ?? ids.next();
      enqueue(encodeFrame({ id, event: name, data }));
    },
    comment(text) {
      if (closed) return;
      const bytes = new TextEncoder().encode(`: ${text}\n\n`);
      enqueue(bytes);
    },
    heartbeat() {
      if (closed) return;
      enqueue(heartbeatFrame());
    },
    done(payload) {
      if (closed) return;
      enqueue(encodeFrame({ event: 'done', data: JSON.stringify(payload ?? null) }));
      runClose();
    },
    close() {
      runClose();
    },
    onClose(fn) {
      if (closed) {
        try { fn(); } catch { /* ignore */ }
        return;
      }
      closeHandlers.push(fn);
    },
    get closed() { return closed; },
  };

  // Pre-wire the abort signal — fires before/during start() if client
  // disconnects before we even open.
  const onAbort = () => runClose();
  if (opts.signal.aborted) {
    // Client already gone; we still return a response, but it'll close
    // immediately after start() runs.
    queueMicrotask(runClose);
  } else {
    opts.signal.addEventListener('abort', onAbort, { once: true });
    // Register a cleanup to remove the listener once we run close (avoids
    // a lingering listener if close fires for other reasons).
    sink.onClose(() => opts.signal.removeEventListener('abort', onAbort));
  }

  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      controllerRef = controller;

      // Lifecycle step 1: terminal short-circuit.
      const term = opts.terminalShortCircuit?.();
      if (term) {
        sink.done(term);
        return;
      }

      // Step 2: initial heartbeat (confirms liveness to the client).
      if (initialHeartbeat) {
        enqueue(heartbeatFrame());
      }

      // Step 3: replay backfill, filtered by lastEventId.
      if (opts.replay) {
        try {
          for (const item of opts.replay()) {
            if (item.id <= sinceId) continue;
            // Advance id allocator so post-replay events don't collide.
            ids.setFloor(item.id);
            enqueue(encodeFrame({
              id: item.id,
              event: item.name,
              data: JSON.stringify(item.data),
            }));
          }
        } catch (err) {
          enqueue(encodeFrame({
            event: 'error',
            data: JSON.stringify({ phase: 'replay', message: String(err instanceof Error ? err.message : err) }),
          }));
          runClose();
          return;
        }
      }

      // Step 4: heartbeat timer.
      if (heartbeatMs > 0) {
        heartbeatTimer = setInterval(() => {
          if (closed) return;
          enqueue(heartbeatFrame());
        }, heartbeatMs);
        if (typeof heartbeatTimer.unref === 'function') heartbeatTimer.unref();
      }

      // Step 5: setup. Async — runs concurrently with anything else the
      // caller registers via sink.event / sink.onClose etc.
      Promise.resolve(opts.setup(sink)).catch((err) => {
        if (closed) return;
        enqueue(encodeFrame({
          event: 'error',
          data: JSON.stringify({ phase: 'setup', message: String(err instanceof Error ? err.message : err) }),
        }));
        runClose();
      });
    },
    cancel() {
      runClose();
    },
  });

  const headers = new Headers(DEFAULT_HEADERS);
  if (opts.headers) {
    const overrides = new Headers(opts.headers);
    overrides.forEach((value, key) => headers.set(key, value));
  }

  return new Response(stream, { headers });
}
