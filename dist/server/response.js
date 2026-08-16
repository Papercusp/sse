"use strict";
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
 *   - Resume-integrity `resync` control event (opt-in via `resumeBounds`): when a
 *     resuming client's id is below the buffer floor (gap) or above the source max
 *     (restart), emit `resync` + skip partial replay → client refetches full
 *   - Single onClose path for all close triggers (cancel, signal abort, done, close)
 *
 * Wire format is fixed (STABILITY CONTRACT — see README). The lifecycle is
 * deterministic; see comments at sseResponse() body for the open-time order.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.bridgeChannel = bridgeChannel;
exports.parseLastEventId = parseLastEventId;
exports.sseResponse = sseResponse;
const format_1 = require("../wire/format");
const ids_1 = require("../wire/ids");
const DEFAULT_HEADERS = {
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
async function bridgeChannel(source, sink, map) {
    const iter = source[Symbol.asyncIterator]();
    sink.onClose(() => { void iter.return?.(undefined); });
    while (!sink.closed) {
        const { value, done } = await iter.next();
        if (done)
            return;
        const out = map(value);
        if (out)
            sink.event(out.name, out.data, out.opts);
    }
}
/** Parse the Last-Event-ID header as a positive integer; null if absent or invalid. */
function parseLastEventId(req) {
    const raw = req.headers.get('Last-Event-ID');
    if (!raw)
        return null;
    const n = Number(raw);
    return Number.isFinite(n) && n >= 0 ? n : null;
}
function sseResponse(opts) {
    const ids = (0, ids_1.createIdAllocator)(opts.lastEventId ?? 0);
    const heartbeatMs = opts.heartbeatMs ?? 10_000;
    const initialHeartbeat = opts.initialHeartbeat ?? true;
    const backpressureTimeoutMs = Math.max(0, opts.backpressureTimeoutMs ?? 30_000);
    const sinceId = opts.lastEventId ?? 0;
    let closed = false;
    let controllerRef = null;
    let heartbeatTimer = null;
    let backpressureTimer = null;
    const closeHandlers = [];
    const clearBackpressureTimer = () => {
        if (!backpressureTimer)
            return;
        clearTimeout(backpressureTimer);
        backpressureTimer = null;
    };
    const trackBackpressure = () => {
        if (closed || !controllerRef || backpressureTimeoutMs === 0)
            return;
        const desiredSize = controllerRef.desiredSize;
        if (desiredSize == null || desiredSize > 0) {
            clearBackpressureTimer();
            return;
        }
        if (backpressureTimer)
            return;
        backpressureTimer = setTimeout(() => {
            backpressureTimer = null;
            if (!closed && controllerRef != null && (controllerRef.desiredSize ?? 1) <= 0) {
                runClose();
            }
        }, backpressureTimeoutMs);
        if (typeof backpressureTimer.unref === 'function')
            backpressureTimer.unref();
    };
    const enqueue = (bytes) => {
        if (closed || !controllerRef)
            return;
        try {
            controllerRef.enqueue(bytes);
            trackBackpressure();
        }
        catch {
            // Controller already closed by upstream; mark closed and run cleanup.
            runClose();
        }
    };
    const runClose = () => {
        if (closed)
            return;
        closed = true;
        if (heartbeatTimer) {
            clearInterval(heartbeatTimer);
            heartbeatTimer = null;
        }
        clearBackpressureTimer();
        // Run handlers in registration order; swallow errors so one bad handler
        // doesn't block the others.
        for (const fn of closeHandlers) {
            try {
                fn();
            }
            catch { /* ignore */ }
        }
        try {
            controllerRef?.close();
        }
        catch { /* already closed */ }
        controllerRef = null;
    };
    const sink = {
        event(name, data, eventOpts) {
            if (closed)
                return;
            const id = eventOpts?.id ?? ids.next();
            enqueue((0, format_1.encodeFrame)({ id, event: name, data: JSON.stringify(data) }));
        },
        eventRaw(name, data, eventOpts) {
            if (closed)
                return;
            const id = eventOpts?.id ?? ids.next();
            enqueue((0, format_1.encodeFrame)({ id, event: name, data }));
        },
        comment(text) {
            if (closed)
                return;
            const bytes = new TextEncoder().encode(`: ${text}\n\n`);
            enqueue(bytes);
        },
        heartbeat() {
            if (closed)
                return;
            enqueue((0, format_1.heartbeatFrame)());
        },
        done(payload) {
            if (closed)
                return;
            enqueue((0, format_1.encodeFrame)({ event: 'done', data: JSON.stringify(payload ?? null) }));
            runClose();
        },
        close() {
            runClose();
        },
        onClose(fn) {
            if (closed) {
                try {
                    fn();
                }
                catch { /* ignore */ }
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
    }
    else {
        opts.signal.addEventListener('abort', onAbort, { once: true });
        // Register a cleanup to remove the listener once we run close (avoids
        // a lingering listener if close fires for other reasons).
        sink.onClose(() => opts.signal.removeEventListener('abort', onAbort));
    }
    const stream = new ReadableStream({
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
                enqueue((0, format_1.heartbeatFrame)());
            }
            // Step 2.5: resume-integrity check. If the client is resuming but its
            // resume point is no longer recoverable from the buffer (events evicted →
            // 'gap', or its id is ahead of the source after a restart → 'ahead'), emit
            // the reserved `resync` control event and SKIP partial replay — the client
            // must discard local state + refetch the full snapshot. This is the
            // standardized fallback-to-full that stops a silent post-eviction divergence.
            let resynced = false;
            if (opts.lastEventId != null && opts.lastEventId > 0 && opts.resumeBounds) {
                const b = opts.resumeBounds();
                if (b) {
                    const reason = opts.lastEventId > b.maxId ? 'ahead' : opts.lastEventId + 1 < b.floorId ? 'gap' : null;
                    if (reason) {
                        enqueue((0, format_1.encodeFrame)({
                            event: 'resync',
                            data: JSON.stringify({ reason, fromId: opts.lastEventId, floorId: b.floorId, maxId: b.maxId }),
                        }));
                        resynced = true;
                    }
                }
            }
            // Step 3: replay backfill, filtered by lastEventId. Skipped after a resync —
            // the client is refetching the full snapshot, so a partial backfill is moot.
            if (opts.replay && !resynced) {
                try {
                    for (const item of opts.replay()) {
                        if (item.id <= sinceId)
                            continue;
                        // Advance id allocator so post-replay events don't collide.
                        ids.setFloor(item.id);
                        enqueue((0, format_1.encodeFrame)({
                            id: item.id,
                            event: item.name,
                            data: JSON.stringify(item.data),
                        }));
                    }
                }
                catch (err) {
                    enqueue((0, format_1.encodeFrame)({
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
                    if (closed)
                        return;
                    enqueue((0, format_1.heartbeatFrame)());
                }, heartbeatMs);
                if (typeof heartbeatTimer.unref === 'function')
                    heartbeatTimer.unref();
            }
            // Step 5: setup. Async — runs concurrently with anything else the
            // caller registers via sink.event / sink.onClose etc.
            Promise.resolve(opts.setup(sink)).catch((err) => {
                if (closed)
                    return;
                enqueue((0, format_1.encodeFrame)({
                    event: 'error',
                    data: JSON.stringify({ phase: 'setup', message: String(err instanceof Error ? err.message : err) }),
                }));
                runClose();
            });
        },
        cancel() {
            runClose();
        },
        pull() {
            // A pull means the consumer is draining again. The next enqueue will
            // re-arm the deadline if it falls behind once more.
            clearBackpressureTimer();
        },
    });
    const headers = new Headers(DEFAULT_HEADERS);
    if (opts.headers) {
        const overrides = new Headers(opts.headers);
        overrides.forEach((value, key) => headers.set(key, value));
    }
    return new Response(stream, { headers });
}
