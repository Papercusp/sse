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

import { registerLiveStream } from './stream-registry';

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
export const DEFAULT_ZOMBIE_TIMEOUT_MS = 45_000;

const DEFAULTS = {
  initialBackoffMs: 1_000,
  maxBackoffMs: 30_000,
  jitter: 0.2,
  zombieTimeoutMs: DEFAULT_ZOMBIE_TIMEOUT_MS,
  maxConsecutiveFailures: 3,
  visibilityPauseMs: 60_000,
};

export function createResilientEventSource(opts: ResilientEventSourceOptions): ResilientEventSource {
  const cfg = { ...DEFAULTS, ...opts };
  const Ctor: typeof EventSource | undefined =
    opts.eventSourceCtor
    ?? (typeof globalThis !== 'undefined' ? (globalThis as { EventSource?: typeof EventSource }).EventSource : undefined);

  let url = opts.url;
  let status: ResilientEventSourceStatus = 'idle';
  let lastEventId: string | null = null;

  let es: EventSource | null = null;
  let backoffMs = cfg.initialBackoffMs;
  let consecutiveFailures = 0;
  let escalated = false;
  let firstConnect = true;
  let cancelled = false;

  let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  let zombieTimer: ReturnType<typeof setTimeout> | null = null;
  let hiddenSinceTimer: ReturnType<typeof setTimeout> | null = null;
  let pausedByVisibility = false;
  let pausedByPageHide = false;
  /** Unregister fn for this stream's per-host connection slot; null when we hold none. */
  let releaseSlot: (() => void) | null = null;
  const dropSlot = () => {
    releaseSlot?.();
    releaseSlot = null;
  };

  const setStatus = (next: ResilientEventSourceStatus) => {
    if (status === next) return;
    status = next;
    opts.onStatusChange?.(next);
  };

  const computeJitter = (ms: number) => {
    const lo = 1 - cfg.jitter;
    const hi = 1 + cfg.jitter;
    return ms * (lo + Math.random() * (hi - lo));
  };

  const clearReconnect = () => {
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
  };
  const clearZombie = () => {
    if (zombieTimer) { clearTimeout(zombieTimer); zombieTimer = null; }
  };

  const scheduleReconnect = () => {
    if (cancelled) return;
    clearReconnect();
    const wait = computeJitter(backoffMs);
    setStatus('failing');
    reconnectTimer = setTimeout(connect, wait);
    backoffMs = Math.min(backoffMs * 2, cfg.maxBackoffMs);
  };

  const armZombieWatchdog = () => {
    if (cfg.zombieTimeoutMs <= 0) return;
    clearZombie();
    zombieTimer = setTimeout(() => {
      if (cancelled) return;
      // No event AND no heartbeat for ZOMBIE_TIMEOUT — force-rebuild even
      // though EventSource may still think it's open.
      es?.close();
      es = null;
      dropSlot();
      scheduleReconnect();
    }, cfg.zombieTimeoutMs);
  };

  /**
   * ONE seam for "something arrived on this connection".
   *
   * The zombie watchdog already had a precise notion of inbound liveness —
   * heartbeats included — but kept it entirely private, so a caller wanting to
   * show "this view may be behind" had to re-derive it from the handlers it can
   * see. Those are payload events, which an IDLE producer never emits, so that
   * derivation reports a healthy-but-quiet stream as dead. Heartbeats are the
   * only signal that separates the two, and they were unobservable: the loop
   * below deliberately skips a caller's `heartbeat` handler because this module
   * consumes it.
   *
   * Routing both through one function is what keeps the caller's notion of
   * liveness identical to the watchdog's rather than a lookalike that can drift.
   */
  const signal = () => {
    armZombieWatchdog();
    try { opts.onSignal?.(); }
    catch { /* an observer must never tear down the stream */ }
  };

  const connect = () => {
    if (cancelled) return;
    if (!Ctor) {
      // No EventSource (SSR / Node without polyfill). Stay idle silently.
      return;
    }
    setStatus('connecting');
    try {
      es = new Ctor(url, opts.withCredentials ? { withCredentials: true } : undefined);
    } catch (e) {
      opts.onError?.(e as Error);
      setStatus('failing');
      scheduleReconnect();
      return;
    }
    // This stream now holds one of the engine's per-host connection slots (see
    // stream-registry: at the cap, every REST fetch on the page starves). Track
    // it from construction — a CONNECTING socket occupies the slot too — and
    // release on every path that drops the socket below.
    releaseSlot?.();
    releaseSlot = registerLiveStream(url);

    es.addEventListener('open', () => {
      backoffMs = cfg.initialBackoffMs;
      consecutiveFailures = 0;
      escalated = false;
      firstConnect = false;
      setStatus('open');
      opts.onOpen?.();
      signal();
    });

    // Heartbeat is liveness only — resets watchdog, no payload to dispatch.
    // It is ALSO the only signal an idle producer emits, which is why `signal`
    // (not a bare watchdog re-arm) is what publishes it to the caller.
    es.addEventListener('heartbeat', () => {
      signal();
    });

    // Wire all user handlers. Each treats receipt as a liveness signal.
    for (const [name, handler] of Object.entries(opts.handlers)) {
      if (name === 'heartbeat') continue; // already handled above
      es.addEventListener(name, (raw) => {
        signal();
        const ev = raw as MessageEvent;
        if (ev.lastEventId) lastEventId = ev.lastEventId;
        try { handler(ev.data as string, ev); }
        catch { /* user-handler errors don't tear down the stream */ }
      });
    }

    // Also catch the default 'message' name in case server emits it.
    if (!('message' in opts.handlers)) {
      es.addEventListener('message', () => signal());
    }

    es.addEventListener('error', () => {
      if (cancelled) return;
      // Escalation counts EVERY error (a server-down stream stays CONNECTING and
      // retries forever — we still want to tell the caller after N so it can
      // fall back to another transport).
      consecutiveFailures++;
      if (
        cfg.maxConsecutiveFailures > 0
        && consecutiveFailures >= cfg.maxConsecutiveFailures
        && !escalated
      ) {
        escalated = true;
        opts.onError?.(new Error(
          `SSE connection to ${url} failed ${consecutiveFailures} consecutive times`,
        ));
      }
      // Recreate ONLY when the underlying EventSource has genuinely given up
      // (readyState CLOSED). A transient drop leaves it at CONNECTING, where the
      // EventSource — native, or IpcEventSource on desktop — reconnects itself
      // with Last-Event-ID; the zombie watchdog armed during the last OPEN is
      // the safety net if that silent reconnect never recovers. Closing +
      // recreating on a CONNECTING error (the old behavior) discards a stream
      // that was about to resume, and over IPC tears down the channel + re-subs
      // the sync layer on every blip (the flashing). Terminal errors set
      // readyState CLOSED and land here for a real reconnect-with-backoff.
      const CLOSED = 2;
      if (!es || (es as { readyState?: number }).readyState === CLOSED) {
        es?.close();
        es = null;
        dropSlot();
        clearZombie();
        scheduleReconnect(); // sets status 'failing' + jittered backoff
      } else {
        // Auto-reconnecting (CONNECTING). Reflect the blip; the `open` handler
        // resets backoff/failures when it recovers.
        setStatus('failing');
      }
    });

    // Silence "unused" lint for the firstConnect flag — preserved for
    // future per-attempt diagnostics (matches SSEAdapter's metrics hook).
    void firstConnect;
  };

  // Visibility pause wiring.
  const onVisibilityChange = () => {
    if (typeof document === 'undefined') return;
    if (document.visibilityState === 'hidden') {
      if (hiddenSinceTimer) return;
      hiddenSinceTimer = setTimeout(() => {
        hiddenSinceTimer = null;
        if (cancelled || !es) return;
        pausedByVisibility = true;
        es.close();
        es = null;
        dropSlot();
        clearZombie();
        clearReconnect();
        setStatus('idle');
      }, cfg.visibilityPauseMs);
    } else {
      if (hiddenSinceTimer) { clearTimeout(hiddenSinceTimer); hiddenSinceTimer = null; }
      if (pausedByVisibility) {
        pausedByVisibility = false;
        backoffMs = cfg.initialBackoffMs;
        connect();
      }
    }
  };

  // No Ctor (SSR / Node without polyfill) → connect() is a permanent no-op,
  // so a visibility listener would be a pure leak (audit P-071): nothing to
  // pause, nothing to resume, and non-browser hosts rarely call close().
  if (cfg.visibilityPause && typeof document !== 'undefined' && Ctor) {
    document.addEventListener('visibilitychange', onVisibilityChange);
  }

  // bfcache lifecycle (2026-07-26, owner desktop pool-starvation): a document
  // put into the back/forward cache is FROZEN, not unmounted — React effect
  // cleanups never run, so this stream's socket stays ESTABLISHED while the
  // page sits in the cache. WebKit keeps ~6 connections per host: a handful of
  // same-origin navigations (each old page caching one live SSE socket)
  // exhausts the pool and every fetch from the LIVE page queues forever
  // (observed: gym tab stuck on "Loading…" with 6/6 sockets held, server
  // healthy at 13ms). `pagehide` is the one reliable signal (fires for both
  // bfcache entry and real unload) → release the socket immediately;
  // `pageshow` with `persisted` restores it. NOT gated on visibilityPause —
  // releasing the socket on page exit is correctness, not an optimization.
  const onPageHide = () => {
    if (cancelled || pausedByPageHide) return;
    pausedByPageHide = true;
    if (hiddenSinceTimer) { clearTimeout(hiddenSinceTimer); hiddenSinceTimer = null; }
    es?.close();
    es = null;
    dropSlot();
    clearZombie();
    clearReconnect();
    setStatus('idle');
  };
  const onPageShow = (ev: { persisted?: boolean }) => {
    if (cancelled || !pausedByPageHide) return;
    pausedByPageHide = false;
    // Only a bfcache restore needs a reconnect; a normal first-show never saw
    // the pagehide close. Guarded anyway (pausedByPageHide) so a stray
    // non-persisted pageshow after a pagehide still recovers the stream.
    void ev;
    backoffMs = cfg.initialBackoffMs;
    connect();
  };
  if (typeof window !== 'undefined' && Ctor) {
    window.addEventListener('pagehide', onPageHide);
    window.addEventListener('pageshow', onPageShow);
  }

  // Kick off the initial connect.
  connect();

  return {
    get status() { return status; },
    get lastEventId() { return lastEventId; },
    setUrl(next: string) {
      if (next === url) return;
      url = next;
      es?.close();
      es = null;
      dropSlot();
      clearZombie();
      clearReconnect();
      backoffMs = cfg.initialBackoffMs;
      consecutiveFailures = 0;
      escalated = false;
      if (!cancelled) connect();
    },
    reconnect() {
      if (cancelled) return;
      es?.close();
      es = null;
      dropSlot();
      clearZombie();
      clearReconnect();
      backoffMs = cfg.initialBackoffMs;
      connect();
    },
    close() {
      cancelled = true;
      es?.close();
      es = null;
      dropSlot();
      clearZombie();
      clearReconnect();
      if (hiddenSinceTimer) { clearTimeout(hiddenSinceTimer); hiddenSinceTimer = null; }
      if (cfg.visibilityPause && typeof document !== 'undefined' && Ctor) {
        document.removeEventListener('visibilitychange', onVisibilityChange);
      }
      if (typeof window !== 'undefined' && Ctor) {
        window.removeEventListener('pagehide', onPageHide);
        window.removeEventListener('pageshow', onPageShow);
      }
      setStatus('closed');
    },
  };
}
