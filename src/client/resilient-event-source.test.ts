import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { createResilientEventSource as createResilientEventSourceRaw } from './resilient-event-source';

// ── Fake EventSource ──────────────────────────────────────────────────────
//
// Mirrors enough of the spec to exercise the wrapper. Tests control opens,
// errors, and named events directly.

interface FakeListenerEntry { type: string; fn: (ev: any) => void; }

class FakeEventSource {
  static instances: FakeEventSource[] = [];
  static lastUrl: string | null = null;

  readonly url: string;
  readonly listeners: FakeListenerEntry[] = [];
  closed = false;
  // 0 CONNECTING, 1 OPEN, 2 CLOSED — the wrapper reads this in its error handler
  // to tell a transient drop (CONNECTING, auto-reconnecting) from a death.
  readyState = 0;

  constructor(url: string) {
    this.url = url;
    FakeEventSource.lastUrl = url;
    FakeEventSource.instances.push(this);
  }
  addEventListener(type: string, fn: (ev: any) => void) {
    this.listeners.push({ type, fn });
  }
  close() { this.closed = true; this.readyState = 2; }

  // Test-only helpers
  fire(type: string, data: any = '', lastEventId?: string) {
    for (const e of this.listeners) {
      if (e.type === type) {
        e.fn({ data, type, lastEventId } as any);
      }
    }
  }
  fireOpen() { this.readyState = 1; this.fire('open'); }
  /** A FATAL error — the ES gave up (readyState CLOSED). The wrapper rebuilds. */
  fireError() { this.readyState = 2; this.fire('error'); }
  /** A TRANSIENT drop — the ES is auto-reconnecting (readyState CONNECTING). */
  fireDrop() { this.readyState = 0; this.fire('error'); }
}

const activeSources: Array<ReturnType<typeof createResilientEventSourceRaw>> = [];
const createResilientEventSource = (...args: Parameters<typeof createResilientEventSourceRaw>) => {
  const source = createResilientEventSourceRaw(...args);
  activeSources.push(source);
  return source;
};

beforeEach(() => {
  FakeEventSource.instances = [];
  FakeEventSource.lastUrl = null;
});

afterEach(() => {
  for (const source of activeSources.splice(0)) source.close();
});

const latest = () => FakeEventSource.instances[FakeEventSource.instances.length - 1]!;

describe('createResilientEventSource — happy path', () => {
  it('constructs an EventSource at the URL on creation', () => {
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      eventSourceCtor: FakeEventSource as any,
    });
    expect(FakeEventSource.lastUrl).toBe('http://x/sse');
    expect(FakeEventSource.instances).toHaveLength(1);
  });

  it('transitions idle → connecting → open on first connect', () => {
    const statuses: string[] = [];
    const handle = createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      eventSourceCtor: FakeEventSource as any,
      onStatusChange: (s) => statuses.push(s),
    });
    expect(handle.status).toBe('connecting');
    latest().fireOpen();
    expect(handle.status).toBe('open');
    expect(statuses).toEqual(['connecting', 'open']);
  });

  it('dispatches named events to handlers with raw data', () => {
    const got: string[] = [];
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: {
        update: (data) => got.push(`u:${data}`),
        invalidate: (data) => got.push(`i:${data}`),
      },
      eventSourceCtor: FakeEventSource as any,
    });
    latest().fireOpen();
    latest().fire('update', '{"x":1}');
    latest().fire('invalidate', '{"y":2}');
    expect(got).toEqual(['u:{"x":1}', 'i:{"y":2}']);
  });

  it('tracks lastEventId from MessageEvent.lastEventId', () => {
    const handle = createResilientEventSource({
      url: 'http://x/sse',
      handlers: { update: () => {} },
      eventSourceCtor: FakeEventSource as any,
    });
    latest().fireOpen();
    latest().fire('update', 'hi', '42');
    expect(handle.lastEventId).toBe('42');
  });

  it('user-handler error does NOT tear down the stream', () => {
    const handle = createResilientEventSource({
      url: 'http://x/sse',
      handlers: { update: () => { throw new Error('user-bug'); } },
      eventSourceCtor: FakeEventSource as any,
    });
    latest().fireOpen();
    expect(() => latest().fire('update', 'hi')).not.toThrow();
    expect(handle.status).toBe('open');
  });
});

describe('createResilientEventSource — reconnect + backoff + jitter', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('reconnects after error with backoff 1s, 2s, 4s, 8s, 16s, 30s (±20% jitter)', () => {
    // Math.random pinned so jitter is deterministic.
    const rand = vi.spyOn(Math, 'random').mockReturnValue(0.5); // mid-band: multiplier = 1.0
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      initialBackoffMs: 1000,
      maxBackoffMs: 30_000,
      maxConsecutiveFailures: 0, // disable escalation so we keep reconnecting
      eventSourceCtor: FakeEventSource as any,
    });
    const expected = [1000, 2000, 4000, 8000, 16_000, 30_000, 30_000];
    let prevCount = 1;
    for (const wait of expected) {
      latest().fireError();
      vi.advanceTimersByTime(wait - 1);
      expect(FakeEventSource.instances.length).toBe(prevCount); // no reconnect yet
      vi.advanceTimersByTime(1);
      expect(FakeEventSource.instances.length).toBe(prevCount + 1);
      prevCount++;
    }
    rand.mockRestore();
  });

  it('applies ±20% jitter (within tolerance)', () => {
    const samples: number[] = [];
    const orig = Math.random;
    // 0..1 sweep — wrapper uses random(1-jitter, 1+jitter) so multipliers are in [0.8, 1.2].
    vi.spyOn(Math, 'random').mockImplementation(() => samples.length / 10);
    for (let i = 0; i < 10; i++) {
      FakeEventSource.instances = [];
      vi.clearAllTimers();
      const source = createResilientEventSource({
        url: 'http://x/sse',
        handlers: {},
        initialBackoffMs: 1000,
        maxConsecutiveFailures: 0,
        eventSourceCtor: FakeEventSource as any,
      });
      // Force one error, observe how long until reconnect fires.
      latest().fireError();
      // Try increasing waits until next connect happens.
      let found = 0;
      for (let t = 1; t <= 1500; t++) {
        vi.advanceTimersByTime(1);
        if (FakeEventSource.instances.length === 2) { found = t; break; }
      }
      samples.push(found);
      // Each iteration is an independent jitter sample. Release its registered
      // connection before starting the next sample so this test does not create
      // the exact standing-stream budget exhaustion the registry warns about.
      source.close();
    }
    Math.random = orig;
    // All samples should fall within [800, 1200] (1000ms ±20%).
    for (const s of samples) {
      expect(s).toBeGreaterThanOrEqual(800);
      expect(s).toBeLessThanOrEqual(1200);
    }
  });

  it('resets backoff to initial on successful open', () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.5);
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      initialBackoffMs: 1000,
      maxConsecutiveFailures: 0,
      eventSourceCtor: FakeEventSource as any,
    });
    // Fail twice → backoff grows to 2s
    latest().fireError();
    vi.advanceTimersByTime(1000);
    latest().fireError();
    vi.advanceTimersByTime(2000);
    expect(FakeEventSource.instances.length).toBe(3);
    // Successful open resets backoff.
    latest().fireOpen();
    latest().fireError();
    vi.advanceTimersByTime(999);
    expect(FakeEventSource.instances.length).toBe(3); // not yet
    vi.advanceTimersByTime(1);
    expect(FakeEventSource.instances.length).toBe(4); // back to 1s
  });
});

describe('createResilientEventSource — transient (CONNECTING) drop cooperates with underlying auto-reconnect', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('does NOT recreate the EventSource on a transient drop (readyState CONNECTING)', () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.5);
    const statuses: string[] = [];
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      initialBackoffMs: 1000,
      maxConsecutiveFailures: 0,
      zombieTimeoutMs: 0, // isolate: only the error-path behavior under test
      eventSourceCtor: FakeEventSource as any,
      onStatusChange: (s) => statuses.push(s),
    });
    latest().fireOpen();
    expect(FakeEventSource.instances).toHaveLength(1);
    // The underlying ES stays CONNECTING and resumes itself — the wrapper must
    // leave it alone (no close, no rebuild) even well past the backoff window.
    latest().fireDrop();
    expect(statuses.at(-1)).toBe('failing');
    vi.advanceTimersByTime(60_000);
    expect(FakeEventSource.instances).toHaveLength(1);
    expect(latest().closed).toBe(false);
  });

  it('recovers to open when the same ES re-opens after a transient drop (no new instance)', () => {
    const handle = createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      maxConsecutiveFailures: 0,
      zombieTimeoutMs: 0,
      eventSourceCtor: FakeEventSource as any,
    });
    latest().fireOpen();
    latest().fireDrop(); // CONNECTING — wrapper waits
    expect(handle.status).toBe('failing');
    latest().fireOpen(); // underlying ES resumed — SAME instance fires open
    expect(handle.status).toBe('open');
    expect(FakeEventSource.instances).toHaveLength(1);
  });

  it('still escalates after maxConsecutiveFailures on CONNECTING drops (server-down)', () => {
    const onError = vi.fn();
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      maxConsecutiveFailures: 3,
      zombieTimeoutMs: 0,
      eventSourceCtor: FakeEventSource as any,
      onError,
    });
    latest().fireDrop();
    latest().fireDrop();
    latest().fireDrop();
    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0]?.[0]?.message).toMatch(/3 consecutive times/);
    // The win: no churn — the wrapper never rebuilt the EventSource.
    expect(FakeEventSource.instances).toHaveLength(1);
  });

  it('zombie watchdog still force-rebuilds if a CONNECTING reconnect never recovers', () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.5);
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      zombieTimeoutMs: 5_000,
      initialBackoffMs: 1000,
      maxConsecutiveFailures: 0,
      eventSourceCtor: FakeEventSource as any,
    });
    latest().fireOpen(); // arms zombie at t=5000
    vi.advanceTimersByTime(4_000);
    latest().fireDrop(); // CONNECTING — no rebuild, and zombie is NOT reset
    expect(FakeEventSource.instances).toHaveLength(1);
    vi.advanceTimersByTime(1_001); // t=5_001 since open → zombie fires → close + reconnect
    expect(latest().closed).toBe(true);
    vi.advanceTimersByTime(1_000); // backoff elapses → new instance
    expect(FakeEventSource.instances).toHaveLength(2);
  });

  it('a CLOSED (fatal) error still rebuilds with backoff', () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.5);
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      initialBackoffMs: 1000,
      maxConsecutiveFailures: 0,
      zombieTimeoutMs: 0,
      eventSourceCtor: FakeEventSource as any,
    });
    latest().fireOpen();
    latest().fireError(); // readyState CLOSED → genuine death
    vi.advanceTimersByTime(1_000);
    expect(FakeEventSource.instances).toHaveLength(2);
  });
});

describe('createResilientEventSource — zombie watchdog', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('force-reconnects after zombieTimeoutMs of silence', () => {
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      zombieTimeoutMs: 5_000,
      maxConsecutiveFailures: 0,
      eventSourceCtor: FakeEventSource as any,
    });
    latest().fireOpen();
    // No events; no heartbeat. Wait the zombie window.
    vi.advanceTimersByTime(5_001);
    // Wrapper has called es.close(), scheduled reconnect.
    // Default initialBackoffMs is 1000; mid-band jitter assumed (Math.random=0.5).
    vi.spyOn(Math, 'random').mockReturnValue(0.5);
    vi.advanceTimersByTime(1_000);
    expect(FakeEventSource.instances.length).toBe(2);
  });

  it('resets watchdog on heartbeat', () => {
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      zombieTimeoutMs: 5_000,
      maxConsecutiveFailures: 0,
      eventSourceCtor: FakeEventSource as any,
    });
    latest().fireOpen();
    vi.advanceTimersByTime(4_000);
    latest().fire('heartbeat');
    vi.advanceTimersByTime(4_000);
    // Should still be alive (would have fired at 5_000 from open without heartbeat).
    expect(FakeEventSource.instances.length).toBe(1);
    vi.advanceTimersByTime(1_001);
    // Now zombie fires (4_000 + 1_001 = 5_001 since last heartbeat).
    expect(FakeEventSource.instances[0]!.closed).toBe(true);
  });

  it('resets watchdog on a user event', () => {
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: { update: () => {} },
      zombieTimeoutMs: 5_000,
      maxConsecutiveFailures: 0,
      eventSourceCtor: FakeEventSource as any,
    });
    latest().fireOpen();
    vi.advanceTimersByTime(4_000);
    latest().fire('update', '{}');
    vi.advanceTimersByTime(4_000);
    expect(FakeEventSource.instances[0]!.closed).toBe(false);
  });

  it('zombieTimeoutMs=0 disables the watchdog', () => {
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      zombieTimeoutMs: 0,
      maxConsecutiveFailures: 0,
      eventSourceCtor: FakeEventSource as any,
    });
    latest().fireOpen();
    vi.advanceTimersByTime(60_000);
    expect(FakeEventSource.instances[0]!.closed).toBe(false);
  });
});

describe('createResilientEventSource — consecutive failure escalation', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('calls onError exactly once after maxConsecutiveFailures', () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.5);
    const onError = vi.fn();
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      maxConsecutiveFailures: 3,
      initialBackoffMs: 100,
      eventSourceCtor: FakeEventSource as any,
      onError,
    });
    // Three errors with no successful open.
    latest().fireError();
    vi.advanceTimersByTime(100);
    latest().fireError();
    vi.advanceTimersByTime(200);
    latest().fireError();
    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0]?.[0]?.message).toMatch(/3 consecutive times/);
    // More failures don't re-call.
    vi.advanceTimersByTime(400);
    latest().fireError();
    expect(onError).toHaveBeenCalledTimes(1);
  });

  it('resets failure count on successful open', () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.5);
    const onError = vi.fn();
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      maxConsecutiveFailures: 3,
      initialBackoffMs: 100,
      eventSourceCtor: FakeEventSource as any,
      onError,
    });
    latest().fireError();
    vi.advanceTimersByTime(100);
    latest().fireError();
    vi.advanceTimersByTime(200);
    latest().fireOpen(); // recover
    latest().fireError();
    vi.advanceTimersByTime(100);
    latest().fireError();
    expect(onError).not.toHaveBeenCalled();
  });

  it('maxConsecutiveFailures=0 disables escalation', () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.5);
    const onError = vi.fn();
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      maxConsecutiveFailures: 0,
      initialBackoffMs: 100,
      eventSourceCtor: FakeEventSource as any,
      onError,
    });
    for (let i = 0; i < 10; i++) {
      latest().fireError();
      vi.advanceTimersByTime(100_000);
    }
    expect(onError).not.toHaveBeenCalled();
  });
});

describe('createResilientEventSource — setUrl / reconnect / close', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('setUrl closes old + opens new immediately', () => {
    const handle = createResilientEventSource({
      url: 'http://x/a',
      handlers: {},
      eventSourceCtor: FakeEventSource as any,
    });
    latest().fireOpen();
    const first = latest();
    handle.setUrl('http://x/b');
    expect(first.closed).toBe(true);
    expect(FakeEventSource.lastUrl).toBe('http://x/b');
    expect(FakeEventSource.instances).toHaveLength(2);
  });

  it('setUrl to same URL is a no-op', () => {
    const handle = createResilientEventSource({
      url: 'http://x/a',
      handlers: {},
      eventSourceCtor: FakeEventSource as any,
    });
    handle.setUrl('http://x/a');
    expect(FakeEventSource.instances).toHaveLength(1);
  });

  it('reconnect() forces a new connection', () => {
    const handle = createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      eventSourceCtor: FakeEventSource as any,
    });
    latest().fireOpen();
    handle.reconnect();
    expect(FakeEventSource.instances).toHaveLength(2);
  });

  it('close() sets status=closed permanently', () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.5);
    const handle = createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      maxConsecutiveFailures: 0,
      eventSourceCtor: FakeEventSource as any,
    });
    handle.close();
    expect(handle.status).toBe('closed');
    // Pending reconnect doesn't fire after close.
    vi.advanceTimersByTime(100_000);
    expect(FakeEventSource.instances).toHaveLength(1);
  });
});

describe('createResilientEventSource — SSR / missing EventSource', () => {
  it('stays idle silently when no EventSource is available', () => {
    const handle = createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      eventSourceCtor: undefined as unknown as typeof EventSource,
    });
    expect(handle.status).toBe('idle');
    expect(FakeEventSource.instances).toHaveLength(0);
  });
});

describe('createResilientEventSource — EventSource ctor throw', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('reports via onError and schedules reconnect', () => {
    const onError = vi.fn();
    class ThrowingCtor {
      constructor() { throw new Error('connection refused'); }
    }
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      maxConsecutiveFailures: 0,
      eventSourceCtor: ThrowingCtor as unknown as typeof EventSource,
      onError,
    });
    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0]?.[0]?.message).toMatch(/connection refused/);
  });
});

/**
 * `onSignal` — the liveness seam the chat pane's staleness detector needs
 * (gui-chat-pane-repaint-2026-08-12 P-003). The distinction it must preserve:
 * an IDLE producer emits no payload events, but its heartbeats keep arriving —
 * so a UI keyed on payload handlers alone reports a perfectly healthy stream as
 * dead. The heartbeat case below is the entire reason this callback exists,
 * because a caller's own `heartbeat` handler is deliberately never dispatched
 * (this module consumes that event), leaving heartbeats otherwise unobservable.
 */
describe('createResilientEventSource — onSignal liveness', () => {
  it('fires on a HEARTBEAT, which no caller handler can otherwise see', () => {
    const onSignal = vi.fn();
    const heartbeatHandler = vi.fn();
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: { heartbeat: heartbeatHandler },
      eventSourceCtor: FakeEventSource as any,
      onSignal,
    });
    latest().fireOpen();
    onSignal.mockClear();
    latest().fire('heartbeat');
    expect(onSignal).toHaveBeenCalledTimes(1);
    // Unchanged contract: the wrapper consumes `heartbeat`, so the caller's own
    // handler still never runs. That is precisely why onSignal is required.
    expect(heartbeatHandler).not.toHaveBeenCalled();
  });

  it('fires on open and on a dispatched payload event', () => {
    const onSignal = vi.fn();
    const backfill = vi.fn();
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: { backfill },
      eventSourceCtor: FakeEventSource as any,
      onSignal,
    });
    latest().fireOpen();
    expect(onSignal).toHaveBeenCalled();
    const afterOpen = onSignal.mock.calls.length;
    latest().fire('backfill', '[]');
    expect(onSignal.mock.calls.length).toBeGreaterThan(afterOpen);
    expect(backfill).toHaveBeenCalledTimes(1);
  });

  it('a throwing observer never tears down the stream', () => {
    const backfill = vi.fn();
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: { backfill },
      eventSourceCtor: FakeEventSource as any,
      onSignal: () => { throw new Error('observer blew up'); },
    });
    latest().fireOpen();
    expect(() => latest().fire('backfill', '[]')).not.toThrow();
    expect(backfill).toHaveBeenCalledTimes(1);
    expect(latest().closed).toBe(false);
  });

  it('is optional — omitting it changes nothing', () => {
    const backfill = vi.fn();
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: { backfill },
      eventSourceCtor: FakeEventSource as any,
    });
    latest().fireOpen();
    expect(() => latest().fire('heartbeat')).not.toThrow();
    latest().fire('backfill', '[]');
    expect(backfill).toHaveBeenCalledTimes(1);
  });
});
