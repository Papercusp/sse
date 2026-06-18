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
   * Default 30_000. Set to 0 to disable.
   * MUST be > server heartbeatMs by enough margin to absorb network jitter
   * (we use 30s for a 15s server heartbeat — one missed beat of grace).
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

const DEFAULTS = {
  initialBackoffMs: 1_000,
  maxBackoffMs: 30_000,
  jitter: 0.2,
  zombieTimeoutMs: 45_000,
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
      scheduleReconnect();
    }, cfg.zombieTimeoutMs);
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

    es.addEventListener('open', () => {
      backoffMs = cfg.initialBackoffMs;
      consecutiveFailures = 0;
      escalated = false;
      firstConnect = false;
      setStatus('open');
      opts.onOpen?.();
      armZombieWatchdog();
    });

    // Heartbeat is liveness only — resets watchdog, no payload to dispatch.
    es.addEventListener('heartbeat', () => {
      armZombieWatchdog();
    });

    // Wire all user handlers. Each treats receipt as a liveness signal.
    for (const [name, handler] of Object.entries(opts.handlers)) {
      if (name === 'heartbeat') continue; // already handled above
      es.addEventListener(name, (raw) => {
        armZombieWatchdog();
        const ev = raw as MessageEvent;
        if (ev.lastEventId) lastEventId = ev.lastEventId;
        try { handler(ev.data as string, ev); }
        catch { /* user-handler errors don't tear down the stream */ }
      });
    }

    // Also catch the default 'message' name in case server emits it.
    if (!('message' in opts.handlers)) {
      es.addEventListener('message', () => armZombieWatchdog());
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
      clearZombie();
      clearReconnect();
      backoffMs = cfg.initialBackoffMs;
      connect();
    },
    close() {
      cancelled = true;
      es?.close();
      es = null;
      clearZombie();
      clearReconnect();
      if (hiddenSinceTimer) { clearTimeout(hiddenSinceTimer); hiddenSinceTimer = null; }
      if (cfg.visibilityPause && typeof document !== 'undefined' && Ctor) {
        document.removeEventListener('visibilitychange', onVisibilityChange);
      }
      setStatus('closed');
    },
  };
}
