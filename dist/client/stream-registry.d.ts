/**
 * Live SSE stream registry + per-host connection-budget warning.
 *
 * WHY THIS EXISTS (owner incident, 2026-07-26/27 — "gym runs never show up").
 * A browser engine allows a fixed number of concurrent connections PER HOST
 * (WebKit: 6, measured in-app: the 7th EventSource never leaves CONNECTING).
 * A standing SSE stream holds one of those slots for its entire life, so a page
 * with 6 standing streams has ZERO slots left and every ordinary REST fetch
 * queues forever — the UI sits on "Loading…" while the server answers the same
 * query in ~13ms. That is invisible from the server side and nearly invisible
 * from the client (no error, just a promise that never settles), which is how it
 * survived for days.
 *
 * On the packaged desktop this is masked: `/api/*` fetches and EventSource ride
 * Tauri IPC (no sockets at all). In the DEV shell there is no sidecar, so IPC is
 * unavailable and everything falls back to HTTP — the 6-slot budget is real, and
 * this registry is what makes exceeding it observable instead of mysterious.
 *
 * Deliberately dependency-free and cheap: a Map of live streams, a monotonic id,
 * and one throttled console warning when the count reaches the danger line. Also
 * exposed as `window.__papercuspStreams()` so an operator/agent can enumerate the
 * live streams of a running page (`tauri-agent-tools eval`) without a rebuild.
 */
export interface LiveStreamRecord {
    id: number;
    url: string;
    /** Host (origin) the stream connects to — the budget is per host. */
    host: string;
    openedAtMs: number;
}
/**
 * Standing streams per host at which we warn. WebKit's cap is 6; warn at 4 so
 * there is still headroom to act (2 slots left for REST) when the log appears.
 */
export declare const STREAM_BUDGET_WARN_AT = 4;
/** Snapshot of the live streams, oldest first. */
export declare function listLiveStreams(): LiveStreamRecord[];
/** How many standing streams currently target `host`. */
export declare function countLiveStreamsForHost(host: string): number;
/**
 * Register a stream that now holds a connection. Returns its unregister fn —
 * call it whenever the socket is released (close, visibility/bfcache pause, a
 * zombie rebuild), so the count tracks REAL socket usage rather than intent.
 */
export declare function registerLiveStream(url: string): () => void;
/** Test seam — drop all registry state. */
export declare function _resetStreamRegistry(): void;
