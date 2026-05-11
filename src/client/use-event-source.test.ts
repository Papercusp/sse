/**
 * @vitest-environment jsdom
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useEventSource } from './use-event-source';

// Reuse the fake-EventSource pattern from the resilient-event-source test
// but expose it via globalThis since useEventSource auto-detects the ctor.

class FakeEventSource {
  static instances: FakeEventSource[] = [];
  static lastUrl: string | null = null;
  readonly url: string;
  readonly listeners: { type: string; fn: (ev: any) => void }[] = [];
  closed = false;
  constructor(url: string) {
    this.url = url;
    FakeEventSource.lastUrl = url;
    FakeEventSource.instances.push(this);
  }
  addEventListener(type: string, fn: (ev: any) => void) { this.listeners.push({ type, fn }); }
  close() { this.closed = true; }
  fire(type: string, data: any = '', lastEventId?: string) {
    for (const e of this.listeners) if (e.type === type) e.fn({ data, type, lastEventId });
  }
  fireOpen() { this.fire('open'); }
}

beforeEach(() => {
  FakeEventSource.instances = [];
  FakeEventSource.lastUrl = null;
  (globalThis as { EventSource?: unknown }).EventSource = FakeEventSource;
});

afterEach(() => {
  delete (globalThis as { EventSource?: unknown }).EventSource;
});

const latest = () => FakeEventSource.instances[FakeEventSource.instances.length - 1]!;

describe('useEventSource', () => {
  it('opens a connection on mount with the given url', () => {
    renderHook(() =>
      useEventSource('http://x/sse', { handlers: { update: () => {} } }),
    );
    expect(FakeEventSource.lastUrl).toBe('http://x/sse');
  });

  it('does NOT connect when url is null', () => {
    const { result } = renderHook(() =>
      useEventSource(null, { handlers: { update: () => {} } }),
    );
    expect(FakeEventSource.instances).toHaveLength(0);
    expect(result.current.status).toBe('idle');
  });

  it('updates status as the connection progresses', () => {
    const { result } = renderHook(() =>
      useEventSource('http://x/sse', { handlers: { update: () => {} } }),
    );
    expect(result.current.status).toBe('connecting');
    act(() => { latest().fireOpen(); });
    expect(result.current.status).toBe('open');
  });

  it('captures lastEventId from event handler dispatch', () => {
    const { result } = renderHook(() =>
      useEventSource('http://x/sse', { handlers: { update: () => {} } }),
    );
    act(() => {
      latest().fireOpen();
      latest().fire('update', '{}', '99');
    });
    expect(result.current.lastEventId).toBe('99');
  });

  it('closes the old source and opens a new one when url changes', () => {
    const { rerender } = renderHook(
      ({ u }) => useEventSource(u, { handlers: { update: () => {} } }),
      { initialProps: { u: 'http://x/a' as string | null } },
    );
    expect(FakeEventSource.instances).toHaveLength(1);
    const first = latest();
    rerender({ u: 'http://x/b' });
    expect(first.closed).toBe(true);
    expect(FakeEventSource.instances).toHaveLength(2);
    expect(FakeEventSource.lastUrl).toBe('http://x/b');
  });

  it('closes the source on unmount', () => {
    const { unmount } = renderHook(() =>
      useEventSource('http://x/sse', { handlers: { update: () => {} } }),
    );
    const es = latest();
    unmount();
    expect(es.closed).toBe(true);
  });

  it('uses stable handler refs — re-render with different handler does not re-subscribe', () => {
    const seen: string[] = [];
    const { rerender } = renderHook(
      ({ tag }: { tag: string }) =>
        useEventSource('http://x/sse', {
          handlers: { update: (data) => seen.push(`${tag}:${data}`) },
        }),
      { initialProps: { tag: 'a' } },
    );
    expect(FakeEventSource.instances).toHaveLength(1);
    act(() => { latest().fireOpen(); latest().fire('update', '1'); });
    rerender({ tag: 'b' }); // changes handler closure
    expect(FakeEventSource.instances).toHaveLength(1); // did NOT re-subscribe
    act(() => { latest().fire('update', '2'); });
    expect(seen).toEqual(['a:1', 'b:2']); // latest closure used
  });

  it('manual reconnect() rebuilds the source', () => {
    const { result } = renderHook(() =>
      useEventSource('http://x/sse', { handlers: { update: () => {} } }),
    );
    act(() => { latest().fireOpen(); });
    act(() => { result.current.reconnect(); });
    expect(FakeEventSource.instances).toHaveLength(2);
  });

  it('manual close() closes and prevents further reconnects', () => {
    const { result } = renderHook(() =>
      useEventSource('http://x/sse', { handlers: {} }),
    );
    act(() => { result.current.close(); });
    expect(result.current.status).toBe('closed');
    expect(latest().closed).toBe(true);
  });

  it('forwards onStatusChange to the caller', () => {
    const onStatusChange = vi.fn();
    renderHook(() =>
      useEventSource('http://x/sse', { handlers: {}, onStatusChange }),
    );
    act(() => { latest().fireOpen(); });
    expect(onStatusChange).toHaveBeenCalledWith('connecting');
    expect(onStatusChange).toHaveBeenCalledWith('open');
  });
});
