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

/* -------------------------------------------------------------------------
 * Cross-realm accounting (WI-2141694).
 *
 * `live` above is module-scoped, so its denominator is the JS REALM. The
 * browser's connection limit is per ORIGIN-per-process. Three same-origin
 * iframes holding 2-3 standing streams each sit at 6-9 sockets against a cap
 * of 6 while every realm reads 2-3 and stays silent — the accountant is
 * structurally blind to the constraint this module exists to police.
 *
 * Realms of one origin are joined with BroadcastChannel, which is itself
 * origin-scoped: exactly the documents that share a connection pool share a
 * channel, and a parent page on another origin neither hears nor pollutes it.
 *
 * There is deliberately NO heartbeat timer. A standing interval in a generic
 * client lib is both a repo-wide lint failure and a thing every embedder then
 * has to own. Freshness is instead maintained by demand: every registration
 * broadcasts a `query`, so at precisely the moments the total matters, every
 * live peer re-announces. A realm that has gone away answers nothing and ages
 * out of the count lazily, with no timer to fire in a torn-down document.
 * ---------------------------------------------------------------------- */

/** Identifies this module realm. Two iframes (or two module records) differ. */
const REALM_ID = `r${Date.now().toString(36)}${Math.random().toString(36).slice(2, 10)}`;

/** Origin-scoped channel name. Shared by every realm of one origin. */
const BUDGET_CHANNEL_NAME = 'papercusp.sse-budget';

/**
 * How long a peer realm's last announcement is trusted. Refreshed by the
 * `query` every registration broadcasts, so a live peer is re-heard whenever
 * the total is about to be consulted; only a genuinely departed realm ages out.
 */
export const PEER_STALE_AFTER_MS = 30_000;

/** Minimal transport, so tests (and non-DOM hosts) need no BroadcastChannel. */
export interface BudgetChannel {
  post(message: unknown): void;
  onMessage(handler: (message: unknown) => void): void;
  close(): void;
}

interface PeerRealm {
  countsByHost: Record<string, number>;
  lastSeenMs: number;
}

const peers = new Map<string, PeerRealm>();

function defaultChannelFactory(): BudgetChannel | null {
  const Ctor = (globalThis as { BroadcastChannel?: typeof BroadcastChannel }).BroadcastChannel;
  if (typeof Ctor !== 'function') return null;
  const bc = new Ctor(BUDGET_CHANNEL_NAME);
  return {
    post: (message) => bc.postMessage(message),
    onMessage: (handler) => {
      bc.onmessage = (ev: MessageEvent) => handler(ev.data);
    },
    close: () => bc.close(),
  };
}

let channelFactory: () => BudgetChannel | null = defaultChannelFactory;
let channel: BudgetChannel | null = null;
let channelReady = false;

/** Streams this realm holds, grouped by host — the payload we announce. */
function localCountsByHost(): Record<string, number> {
  const out: Record<string, number> = {};
  for (const r of live.values()) out[r.host] = (out[r.host] ?? 0) + 1;
  return out;
}

function announce(): void {
  channel?.post({ t: 'announce', realm: REALM_ID, counts: localCountsByHost() });
}

function onBudgetMessage(raw: unknown): void {
  if (!raw || typeof raw !== 'object') return;
  const msg = raw as { t?: unknown; realm?: unknown; counts?: unknown };
  if (typeof msg.realm !== 'string' || msg.realm === REALM_ID) return;

  if (msg.t === 'bye') {
    peers.delete(msg.realm);
    return;
  }
  if (msg.t === 'query') {
    // Someone is about to make a budget decision — tell them what we hold.
    announce();
    return;
  }
  if (msg.t === 'announce') {
    const counts = (msg.counts ?? {}) as Record<string, number>;
    peers.set(msg.realm, { countsByHost: { ...counts }, lastSeenMs: Date.now() });
    // A peer's streams can push US over the line even though nothing changed
    // locally — that is the whole class of starvation this fix exists to see.
    for (const host of new Set([...Object.keys(counts), ...Object.keys(localCountsByHost())])) {
      maybeWarnForHost(host);
    }
  }
}

function ensureChannel(): void {
  if (channelReady) return;
  channelReady = true;
  try {
    channel = channelFactory();
  } catch {
    channel = null; // no transport — degrade to single-realm accounting
  }
  if (!channel) return;
  try {
    channel.onMessage(onBudgetMessage);
    // Pull peers' current holdings. We do not announce here — callers that
    // change our own count announce explicitly, so a realm that merely reads
    // the budget never advertises itself as a stream holder.
    channel.post({ t: 'query', realm: REALM_ID });
  } catch {
    /* a transport that refuses to wire up is the same as none */
  }
}

/** Drop peers we have not heard from recently. Lazy — no timer anywhere. */
function pruneStalePeers(nowMs: number): void {
  const cutoff = nowMs - PEER_STALE_AFTER_MS;
  for (const [realm, p] of peers) if (p.lastSeenMs < cutoff) peers.delete(realm);
}

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

/**
 * How many standing streams THIS realm holds against `host`.
 *
 * Deliberately local: a caller asking what it can release needs its own set.
 * For a budget decision use {@link countStreamsForHostAllRealms} — the browser
 * caps connections per origin-per-process, not per document.
 */
export function countLiveStreamsForHost(host: string): number {
  let n = 0;
  for (const r of live.values()) if (r.host === host) n += 1;
  return n;
}

/**
 * How many standing streams every known realm of this origin holds against
 * `host` — the number that is actually measured against the browser's cap.
 *
 * Equals {@link countLiveStreamsForHost} when no peers are reachable (no
 * BroadcastChannel, a single document, or a non-DOM host), so single-realm
 * behaviour is unchanged.
 */
export function countStreamsForHostAllRealms(host: string): number {
  pruneStalePeers(Date.now());
  let n = countLiveStreamsForHost(host);
  for (const p of peers.values()) n += p.countsByHost[host] ?? 0;
  return n;
}

export interface StreamBudgetSnapshot {
  host: string;
  /** Streams held by this document. */
  local: number;
  /** Streams held by every other known document of this origin. */
  peer: number;
  /** local + peer — the figure the browser's per-origin cap applies to. */
  total: number;
  warnAt: number;
  /** Other realms currently contributing to `peer`. 0 ⇒ accounting is local-only. */
  peerRealms: number;
  realmId: string;
}

/**
 * The budget as this realm currently understands it. Exposed for diagnostics
 * and as the input a future yield-on-contention policy needs — deciding WHICH
 * stream to drop requires knowing the total first, which is why accounting
 * lands before policy.
 */
export function getStreamBudgetSnapshot(host: string): StreamBudgetSnapshot {
  pruneStalePeers(Date.now());
  const local = countLiveStreamsForHost(host);
  let peer = 0;
  for (const p of peers.values()) peer += p.countsByHost[host] ?? 0;
  return {
    host,
    local,
    peer,
    total: local + peer,
    warnAt: STREAM_BUDGET_WARN_AT,
    peerRealms: peers.size,
    realmId: REALM_ID,
  };
}

/**
 * Warn once per host when the ORIGIN-WIDE total reaches the danger line.
 * Naming the split matters: "4 streams, 2 of them in this document" is what
 * tells a reader the problem is cross-document, which is exactly the fact the
 * per-realm count hid.
 */
function maybeWarnForHost(host: string): void {
  const { local, total, peerRealms } = getStreamBudgetSnapshot(host);
  if (total < STREAM_BUDGET_WARN_AT || warnedHosts.has(host)) return;
  warnedHosts.add(host);
  const urls = listLiveStreams()
    .filter((r) => r.host === host)
    .map((r) => r.url)
    .join(', ');
  const spread =
    peerRealms > 0
      ? ` across ${peerRealms + 1} documents of this origin (${local} in this one)`
      : '';
  // eslint-disable-next-line no-console
  console.warn(
    `[sse-budget] ${total} standing SSE streams open to ${host}${spread}. Browser engines ` +
      `allow ~6 connections per host, and each standing stream holds one for its whole life — ` +
      `at the cap, ordinary REST fetches queue forever ("Loading…" that never resolves). ` +
      `Streams in this document: ${urls}. Enumerate live: window.__papercuspStreams(); ` +
      `budget: window.__papercuspStreamBudget('${host}').`,
  );
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
  ensureChannel();
  // A registration is exactly when the origin-wide total is about to matter:
  // publish what we now hold, and ask peers to restate theirs so the figure we
  // warn on is current rather than remembered.
  channel?.post({ t: 'query', realm: REALM_ID });
  announce();
  maybeWarnForHost(host);
  return () => {
    live.delete(id);
    announce();
    // Re-arm the warning once the host drops back under the line, so a later
    // regression warns again instead of being swallowed by the first one.
    if (countStreamsForHostAllRealms(host) < STREAM_BUDGET_WARN_AT) warnedHosts.delete(host);
  };
}

/** Test seam — drop all registry state, including cross-realm accounting. */
export function _resetStreamRegistry(): void {
  live.clear();
  warnedHosts.clear();
  nextId = 1;
  peers.clear();
  try {
    channel?.close();
  } catch {
    /* closing a spent transport must never fail a reset */
  }
  channel = null;
  channelReady = false;
}

/**
 * Test seam — supply the cross-realm transport.
 *
 * Required because jsdom does not implement BroadcastChannel, so the only way
 * to exercise multi-realm accounting deterministically is to inject a bus.
 * Pass `null` to restore the real one.
 */
export function _setBudgetChannelFactory(factory: (() => BudgetChannel | null) | null): void {
  channelFactory = factory ?? defaultChannelFactory;
  try {
    channel?.close();
  } catch {
    /* ignore */
  }
  channel = null;
  channelReady = false;
}

// Expose for live inspection from a running page (devtools or an agent's
// bridge-eval). Assigned once, defensively — never throws in a non-DOM host.
try {
  if (typeof window !== 'undefined') {
    const w = window as unknown as {
      __papercuspStreams?: () => LiveStreamRecord[];
      __papercuspStreamBudget?: (host?: string) => StreamBudgetSnapshot[];
    };
    w.__papercuspStreams = listLiveStreams;
    // The cross-realm figure: what `__papercuspStreams()` alone cannot show,
    // because it enumerates only THIS document's share of the origin's pool.
    w.__papercuspStreamBudget = (host?: string) => {
      const hosts = host
        ? [host]
        : [...new Set([...Object.keys(localCountsByHost()), ...peerHostNames()])];
      return hosts.map((h) => getStreamBudgetSnapshot(h));
    };
    // Leaving without a goodbye would leave our streams counted against peers
    // until they age out; `pagehide` covers bfcache and ordinary navigation.
    window.addEventListener('pagehide', () => {
      try {
        channel?.post({ t: 'bye', realm: REALM_ID });
      } catch {
        /* the document is going away regardless */
      }
    });
  }
} catch {
  /* non-DOM host — registry still works in-process */
}

function peerHostNames(): string[] {
  const out: string[] = [];
  for (const p of peers.values()) out.push(...Object.keys(p.countsByHost));
  return out;
}
