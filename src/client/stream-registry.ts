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
export const STREAM_BUDGET_WARN_AT = 4;

const live = new Map<number, LiveStreamRecord>();
let nextId = 1;
const warnedHosts = new Set<string>();

function hostOf(url: string): string {
  try {
    return new URL(url, typeof location !== 'undefined' ? location.href : 'http://localhost').host;
  } catch {
    return '<unparsed>';
  }
}

/** Snapshot of the live streams, oldest first. */
export function listLiveStreams(): LiveStreamRecord[] {
  return [...live.values()].sort((a, b) => a.openedAtMs - b.openedAtMs);
}

/** How many standing streams currently target `host`. */
export function countLiveStreamsForHost(host: string): number {
  let n = 0;
  for (const r of live.values()) if (r.host === host) n += 1;
  return n;
}

/**
 * Register a stream that now holds a connection. Returns its unregister fn —
 * call it whenever the socket is released (close, visibility/bfcache pause, a
 * zombie rebuild), so the count tracks REAL socket usage rather than intent.
 */
export function registerLiveStream(url: string): () => void {
  const id = nextId++;
  const host = hostOf(url);
  live.set(id, { id, url, host, openedAtMs: Date.now() });
  const n = countLiveStreamsForHost(host);
  if (n >= STREAM_BUDGET_WARN_AT && !warnedHosts.has(host)) {
    warnedHosts.add(host);
    const urls = listLiveStreams()
      .filter((r) => r.host === host)
      .map((r) => r.url)
      .join(', ');
    // eslint-disable-next-line no-console
    console.warn(
      `[sse-budget] ${n} standing SSE streams open to ${host}. Browser engines allow ~6 ` +
        `connections per host, and each standing stream holds one for its whole life — at the ` +
        `cap, ordinary REST fetches queue forever ("Loading…" that never resolves). ` +
        `Streams: ${urls}. Enumerate live: window.__papercuspStreams().`,
    );
  }
  return () => {
    live.delete(id);
    // Re-arm the warning once the host drops back under the line, so a later
    // regression warns again instead of being swallowed by the first one.
    if (countLiveStreamsForHost(host) < STREAM_BUDGET_WARN_AT) warnedHosts.delete(host);
  };
}

/** Test seam — drop all registry state. */
export function _resetStreamRegistry(): void {
  live.clear();
  warnedHosts.clear();
  nextId = 1;
}

// Expose for live inspection from a running page (devtools or an agent's
// bridge-eval). Assigned once, defensively — never throws in a non-DOM host.
try {
  if (typeof window !== 'undefined') {
    (window as unknown as { __papercuspStreams?: () => LiveStreamRecord[] }).__papercuspStreams =
      listLiveStreams;
  }
} catch {
  /* non-DOM host — registry still works in-process */
}
