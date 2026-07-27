/**
 * Visibility-pause behavior of the resilient EventSource wrapper.
 *
 * The wrapper closes the underlying EventSource after document.hidden
 * persists longer than `visibilityPauseMs`, sets status to 'idle', and
 * reopens on visibilitychange-back-to-visible.
 *
 * This test backs the SSEAdapter visibility-pause metric fix
 * (libs/sync/src/transports/sse/SSEAdapter.tsx): without an 'idle'
 * status transition fired by the wrapper, the adapter's onStatusChange
 * predicate `if (s === 'idle' && !firstConnect) sseDisconnected()`
 * would never fire — silently under-counting tab-hidden idleness.
 *
 * Runs in jsdom (per environmentMatchGlobs in vitest.config.ts).
 *
 * @vitest-environment jsdom
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { createResilientEventSource } from './resilient-event-source';

class FakeEventSource {
  static instances: FakeEventSource[] = [];
  static lastUrl: string | null = null;
  readonly url: string;
  readonly listeners: Array<{ type: string; fn: (ev: any) => void }> = [];
  closed = false;
  constructor(url: string) {
    this.url = url;
    FakeEventSource.lastUrl = url;
    FakeEventSource.instances.push(this);
  }
  addEventListener(type: string, fn: (ev: any) => void) {
    this.listeners.push({ type, fn });
  }
  close() { this.closed = true; }
  fire(type: string, data: any = '') {
    for (const e of this.listeners) if (e.type === type) e.fn({ data, type } as any);
  }
  fireOpen() { this.fire('open'); }
}

const latest = () => FakeEventSource.instances[FakeEventSource.instances.length - 1]!;

function setVisibilityState(state: 'visible' | 'hidden') {
  Object.defineProperty(document, 'visibilityState', { value: state, configurable: true });
  document.dispatchEvent(new Event('visibilitychange'));
}

beforeEach(() => {
  FakeEventSource.instances = [];
  FakeEventSource.lastUrl = null;
  setVisibilityState('visible');
  vi.useFakeTimers();
});
afterEach(() => { vi.useRealTimers(); });

describe('createResilientEventSource — visibility pause', () => {
  it('does NOT register a visibilitychange listener when no EventSource ctor exists (P-071)', () => {
    const realEventSource = (globalThis as { EventSource?: unknown }).EventSource;
    delete (globalThis as { EventSource?: unknown }).EventSource;
    const addSpy = vi.spyOn(document, 'addEventListener');
    try {
      const source = createResilientEventSource({
        url: 'http://x/sse',
        visibilityPause: true,
        handlers: {},
        // no eventSourceCtor, no global → connect() is a permanent no-op
      });
      expect(
        addSpy.mock.calls.filter(([type]) => type === 'visibilitychange'),
      ).toHaveLength(0);
      source.close(); // still safe to close without a registered listener
    } finally {
      addSpy.mockRestore();
      if (realEventSource !== undefined) {
        (globalThis as { EventSource?: unknown }).EventSource = realEventSource;
      }
    }
  });

  it('does NOT close the source when visibility pause is disabled (default)', () => {
    const onStatusChange = vi.fn();
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      eventSourceCtor: FakeEventSource as any,
      onStatusChange,
    });
    latest().fireOpen();
    setVisibilityState('hidden');
    vi.advanceTimersByTime(120_000); // 2 minutes hidden
    expect(latest().closed).toBe(false);
    expect(onStatusChange).not.toHaveBeenCalledWith('idle');
  });

  it('closes the source and transitions to idle after visibilityPauseMs of hidden time', () => {
    const onStatusChange = vi.fn();
    const handle = createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      visibilityPause: true,
      visibilityPauseMs: 5000,
      eventSourceCtor: FakeEventSource as any,
      onStatusChange,
    });
    latest().fireOpen();
    expect(handle.status).toBe('open');

    setVisibilityState('hidden');
    // Not yet past threshold.
    vi.advanceTimersByTime(4000);
    expect(latest().closed).toBe(false);
    // Past threshold.
    vi.advanceTimersByTime(2000);
    expect(latest().closed).toBe(true);
    expect(handle.status).toBe('idle');
    expect(onStatusChange).toHaveBeenCalledWith('idle');
  });

  it('does not pause if visibility goes visible again before threshold', () => {
    const onStatusChange = vi.fn();
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      visibilityPause: true,
      visibilityPauseMs: 5000,
      eventSourceCtor: FakeEventSource as any,
      onStatusChange,
    });
    latest().fireOpen();

    setVisibilityState('hidden');
    vi.advanceTimersByTime(2000);
    setVisibilityState('visible');
    vi.advanceTimersByTime(10_000);
    expect(latest().closed).toBe(false);
    expect(onStatusChange).not.toHaveBeenCalledWith('idle');
  });

  it('reopens on visibilitychange-back-to-visible after a pause', () => {
    const onStatusChange = vi.fn();
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      visibilityPause: true,
      visibilityPauseMs: 5000,
      initialBackoffMs: 100,
      eventSourceCtor: FakeEventSource as any,
      onStatusChange,
    });
    latest().fireOpen();
    expect(FakeEventSource.instances.length).toBe(1);

    setVisibilityState('hidden');
    vi.advanceTimersByTime(6000);
    expect(latest().closed).toBe(true);
    expect(onStatusChange).toHaveBeenLastCalledWith('idle');

    // Visible again — wrapper reopens (status → connecting → open).
    setVisibilityState('visible');
    expect(FakeEventSource.instances.length).toBeGreaterThanOrEqual(2);
    expect(onStatusChange).toHaveBeenLastCalledWith('connecting');
  });
});

describe('createResilientEventSource — bfcache pagehide/pageshow', () => {
  it('releases the socket on pagehide even without visibilityPause (correctness, not an optimization)', () => {
    const onStatusChange = vi.fn();
    createResilientEventSource({
      url: 'http://x/sse',
      handlers: {},
      eventSourceCtor: FakeEventSource as any,
      onStatusChange,
    });
    latest().fireOpen();

    window.dispatchEvent(new Event('pagehide'));
    expect(latest().closed).toBe(true);
    expect(onStatusChange).toHaveBeenLastCalledWith('idle');
  });

  // NOTE: wrappers created by EARLIER tests in this file are never closed and
  // (correctly) also react to window-level pagehide/pageshow — so these tests
  // count only instances carrying their own unique URL instead of asserting
  // on the global instance count.
  const withUrl = (url: string) => FakeEventSource.instances.filter((i) => i.url === url);
  const latestWithUrl = (url: string) => withUrl(url)[withUrl(url).length - 1]!;

  it('reconnects on pageshow after a pagehide pause (bfcache restore)', () => {
    const url = 'http://x/sse-bfc-restore';
    createResilientEventSource({
      url,
      handlers: {},
      initialBackoffMs: 100,
      eventSourceCtor: FakeEventSource as any,
    });
    latestWithUrl(url).fireOpen();
    expect(withUrl(url).length).toBe(1);

    window.dispatchEvent(new Event('pagehide'));
    expect(latestWithUrl(url).closed).toBe(true);

    window.dispatchEvent(new Event('pageshow'));
    expect(withUrl(url).length).toBe(2);
    expect(latestWithUrl(url).closed).toBe(false);
  });

  it('pageshow without a preceding pagehide is a no-op (normal first show)', () => {
    const url = 'http://x/sse-bfc-noop';
    createResilientEventSource({
      url,
      handlers: {},
      eventSourceCtor: FakeEventSource as any,
    });
    latestWithUrl(url).fireOpen();
    window.dispatchEvent(new Event('pageshow'));
    expect(withUrl(url).length).toBe(1);
  });

  it('close() removes the pagehide/pageshow listeners', () => {
    const url = 'http://x/sse-bfc-closed';
    const source = createResilientEventSource({
      url,
      handlers: {},
      eventSourceCtor: FakeEventSource as any,
    });
    latestWithUrl(url).fireOpen();
    source.close();
    window.dispatchEvent(new Event('pagehide'));
    window.dispatchEvent(new Event('pageshow'));
    expect(withUrl(url).length).toBe(1); // no zombie reconnect after close()
  });
});
