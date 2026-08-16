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
export interface SseSink<TEvents extends Record<string, unknown> = Record<string, unknown>> {
    /**
     * Emit a named event. Auto-monotonic id unless explicit `id` provided.
     * Data is JSON-stringified. No-op after close.
     */
    event<K extends keyof TEvents & string>(name: K, data: TEvents[K], opts?: {
        id?: number | string;
    }): void;
    /**
     * Emit a named event with pre-formatted string data (NO JSON.stringify wrap).
     * Use for line-streaming or raw-text protocols (e.g. LLM token deltas).
     * For most cases prefer `event()` — this is the escape hatch.
     */
    eventRaw(name: string, data: string, opts?: {
        id?: number | string;
    }): void;
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
    /** Heartbeat interval ms; 0 disables. Default 10_000 — P2-3
     *  (operator-scalability-event-loop-2026-06-16): a faster beat means a
     *  momentarily-lagged loop is likelier to still emit one within the client's
     *  (now wider) zombie-grace window, instead of tripping a false reconnect. */
    heartbeatMs?: number;
    /** Send a heartbeat at open to confirm liveness. Default true. */
    initialHeartbeat?: boolean;
    /**
     * Close when the response body remains backpressured for this long. Default
     * 30_000ms; set 0 to disable.
     *
     * A direct Web `Response` consumer can read a few frames and then abandon
     * the reader without calling `cancel()`. In that state neither the request
     * signal nor the stream's cancel hook fires, so a long-lived setup loop
     * otherwise keeps producing forever and the unread body queue grows without
     * bound. Normal HTTP adapters continuously drain this Web stream and apply
     * socket backpressure separately, so sustained `desiredSize <= 0` here means
     * the response itself has no active consumer.
     */
    backpressureTimeoutMs?: number;
    /**
     * Optional ring-buffer replay. Called once at open AFTER lastEventId is
     * resolved; lib filters items whose id <= lastEventId.
     */
    replay?: () => Iterable<{
        name: keyof TEvents & string;
        data: TEvents[keyof TEvents];
        id: number;
    }>;
    /**
     * Optional resume-INTEGRITY bounds (the source's current [floorId, maxId] —
     * for a ring buffer: `{ floorId: ch.recent[0].id, maxId: ch.recent.at(-1).id }`).
     * When the client resumes (lastEventId set), the lib checks whether the resume
     * point is still recoverable from replay:
     *   - `lastEventId + 1 < floorId` → the events the client needs were EVICTED
     *     from the buffer (a silent GAP — replay would skip them);
     *   - `lastEventId > maxId`       → the client's id is AHEAD of the source
     *     (the channel/process restarted and ids reset — the client's baseline is
     *     from a dead generation, so future lower ids would be filtered out).
     * In either case the client cannot safely resume, so the lib emits the
     * reserved `resync` control event (`{ reason: 'gap' | 'ahead', fromId, floorId,
     * maxId }`) and SKIPS the partial replay — the client must DISCARD local state
     * and refetch the full snapshot, then resume live. Without it, a resume past the
     * buffer floor (or after a restart) silently diverges — the exact wrong-merge
     * this protocol exists to retire. OPT-IN: omit it (or for append-only streams
     * with no full-refetch fallback) ⇒ no integrity check, legacy replay behaviour.
     */
    resumeBounds?: () => {
        floorId: number;
        maxId: number;
    } | null;
    /**
     * If the source is already terminal at open, emit `done` with the returned
     * payload and close immediately. Client doesn't hang waiting for events
     * that will never come.
     */
    terminalShortCircuit?: () => null | {
        reason: string;
        payload?: unknown;
    };
    /** Extra response headers (merged over defaults). */
    headers?: HeadersInit;
}
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
export declare function bridgeChannel<TItem, TEvents extends Record<string, unknown>>(source: AsyncIterable<TItem>, sink: SseSink<TEvents>, map: (item: TItem) => {
    name: keyof TEvents & string;
    data: TEvents[keyof TEvents & string];
    opts?: {
        id?: number | string;
    };
} | null): Promise<void>;
/** Parse the Last-Event-ID header as a positive integer; null if absent or invalid. */
export declare function parseLastEventId(req: Request): number | null;
export declare function sseResponse<TEvents extends Record<string, unknown> = Record<string, unknown>>(opts: SseResponseOptions<TEvents>): Response;
