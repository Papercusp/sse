/**
 * createResilientEventSource — production-grade EventSource wrapper.
 *
 * Extracted from libs/sync/src/transports/sse/SSEAdapter.tsx (lines 116-292).
 * Framework-free; no React, no react-query, no metrics dep.
 *
 * Resilience features (all of which are load-bearing on desktop):
 *   - Exponential backoff with ±20% jitter (thundering-herd avoidance)
 *   - Zombie watchdog: force-reconnect when no event AND no heartbeat for N ms
 *     (catches proxy-hung-the-stream case where EventSource thinks it's open)
 *   - Consecutive-failure escalation via `onError` so callers can fall back
 *     to a different transport
 *   - Optional visibility-pause: closes EventSource after document is hidden
 *     long enough; reconnects on return (saves battery on background tabs)
 *
 * Spec note: the browser's built-in EventSource auto-reconnects on an ordinary
 * drop (readyState → CONNECTING) using its own ~3s retry. We COOPERATE with
 * that rather than override it: a transient `error` (readyState CONNECTING) is
 * left to the underlying EventSource — native, or on desktop IpcEventSource —
 * to resume with `Last-Event-ID`, while this wrapper owns the heavier policies
 * the browser lacks: zombie watchdog (proxy-hung stream), consecutive-failure
 * escalation, visibility-pause, and a jittered exponential backoff for genuine
 * CLOSED failures (non-2xx / wrong content-type / IPC-unavailable). The old
 * behavior — close + recreate on EVERY error — churned a fresh connection on
 * each blip, and over Tauri IPC a whole new channel + sync re-subscribe + a
 * re-render (the dev-IPC "constant flashing"). The server SHOULD NOT emit
 * `retry:`. Plan: calltool-endpoint-seam-2026-06-01 (Phase D, P-007 / D-006).
 */
export type ResilientEventSourceStatus = 'idle' | 'connecting' | 'open' | 'failing' | 'closed';
export interface ResilientEventSourceOptions {
    url: string;
    /** Send credentials (cookies). Default false. */
    withCredentials?: boolean;
    /** Initial backoff ms. Default 1_000. */
    initialBackoffMs?: number;
    /** Max backoff ms. Default 30_000. */
    maxBackoffMs?: number;
    /** Jitter ratio applied to each backoff. Default 0.2 (±20%). */
    jitter?: number;
    /**
     * Zombie watchdog. If no event AND no heartbeat for this long, force-reconnect.
     * Default `DEFAULT_ZOMBIE_TIMEOUT_MS` (45_000). Set to 0 to disable.
     *
     * MUST be > server heartbeatMs by enough margin to absorb network jitter: at
     * 45s against a 15s server heartbeat that is two missed beats of grace.
     *
     * It is also a CEILING for any consumer running its own silence timer on
     * `onSignal`: this watchdog and that timer are fed by the SAME signal, so
     * whichever is shorter fires first and the other is dead code. A consumer
     * threshold >= this value can never be observed. See
     * `apps/operator/app/_components/chat/stream-freshness.ts`, whose ladder
     * ordering is pinned by `stream-freshness.ladder.test.ts`.
     */
    zombieTimeoutMs?: number;
    /**
     * After this many consecutive failures with zero successful opens,
     * call `onError` once so the caller can escalate (e.g. transport fallback).
     * Successful open resets this. Default 3. Set to 0 to disable escalation.
     */
    maxConsecutiveFailures?: number;
    /** Per-event handlers. Receives raw `data` string (caller parses). */
    handlers: Record<string, (data: string, ev: MessageEvent) => void>;
    /** Called on successful EventSource `open`. */
    onOpen?: () => void;
    /** Called when status changes. */
    onStatusChange?: (status: ResilientEventSourceStatus) => void;
    /**
     * Called on EVERY inbound signal on the connection — open, heartbeat, and any
     * dispatched event — i.e. exactly what the zombie watchdog counts as proof the
     * stream is alive.
     *
     * For a UI that must distinguish "the producer is idle" from "this view has
     * gone deaf", this is the signal to use: payload handlers alone cannot tell
     * them apart, because an idle producer emits none while its heartbeats keep
     * arriving. Note a caller's own `heartbeat` handler is NEVER dispatched (this
     * module consumes that event), so this callback is the only way to observe it.
     *
     * Fires often (≥ once per server heartbeat), so keep it cheap — record a
     * timestamp, do not re-render on every call. Throwing is swallowed.
     */
    onSignal?: () => void;
    /**
     * Called when maxConsecutiveFailures is hit, or when EventSource
     * construction throws. NOT called for normal reconnect cycles.
     */
    onError?: (err: Error) => void;
    /**
     * If true, close EventSource after document.hidden for `visibilityPauseMs`;
     * reopen on visibilitychange. Default false.
     */
    visibilityPause?: boolean;
    /** Delay before pausing when document hidden. Default 60_000. */
    visibilityPauseMs?: number;
    /**
     * Inject an EventSource constructor — for testing with a fake. Default
     * `globalThis.EventSource` (browser) or undefined (Node, returns no-op handle).
     */
    eventSourceCtor?: typeof EventSource;
}
export interface ResilientEventSource {
    readonly status: ResilientEventSourceStatus;
    readonly lastEventId: string | null;
    /** Replace URL — clean close + new connect. */
    setUrl(url: string): void;
    /** Manually trigger a reconnect (cancels current backoff). */
    reconnect(): void;
    /** Clean shutdown. After close, status='closed' permanently. */
    close(): void;
}
/**
 * How long an ostensibly-open stream may deliver NOTHING — not even a heartbeat
 * — before the zombie watchdog force-rebuilds it and publishes `'failing'`.
 *
 * EXPORTED BECAUSE IT IS A CEILING, not merely a default. Any UI that runs its
 * own silence timer to say "this view may be behind" is racing this one, and
 * loses: once this elapses the transport closes the socket and reports
 * `'failing'`, which every such UI renders as reconnecting-or-worse. A UI
 * threshold at or above this value is therefore UNREACHABLE — its state can
 * never be entered, and unit tests that inject the inputs directly will not
 * notice, because the unreachable combination is trivially constructible by
 * hand.
 *
 * That is not hypothetical: the chat pane's staleness banner shipped with a 60s
 * threshold on 2026-08-12 and was dead code from the first commit for exactly
 * this reason (EI-20265888603098901). Consumers must key off this constant and
 * stay strictly below it — `stream-freshness.ts` has the guard test.
 */
export declare const DEFAULT_ZOMBIE_TIMEOUT_MS = 45000;
/**
 * Effective idle ceiling for long-lived SSE calls over the Tauri endpoint-IPC
 * transport. This is a TypeScript mirror of
 * `IPC_STREAM_IDLE_TIMEOUT_MS` in
 * `papercusp-desktop/src-tauri/src/endpoint_ipc.rs`:
 * two 15 s server-heartbeat intervals plus one 2 s watchdog tick.
 *
 * It is exported because a desktop UI's silence rung races this ceiling even
 * though the generic EventSource wrapper's own watchdog defaults to 45 s. The
 * chat freshness ladder source-pins this value against the Rust writer.
 */
export declare const DESKTOP_IPC_STREAM_IDLE_TIMEOUT_MS = 32000;
export declare function createResilientEventSource(opts: ResilientEventSourceOptions): ResilientEventSource;
