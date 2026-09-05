import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  createCrossTabControlStream,
  type ControlBroadcastChannel,
  type ControlDocumentLike,
  type ControlWindowLike,
  type CrossTabControlStream,
} from './cross-tab-control-stream';

/** A tiny asynchronous BroadcastChannel stand-in. */
class FakeBroadcastChannel implements ControlBroadcastChannel {
  static channels = new Map<string, Set<FakeBroadcastChannel>>();
  static instances: FakeBroadcastChannel[] = [];
  readonly name: string;
  readonly listeners = new Set<(event: { data: unknown }) => void>();
  closed = false;
  muted = false;

  constructor(name: string) {
    this.name = name;
    const peers = FakeBroadcastChannel.channels.get(name) ?? new Set<FakeBroadcastChannel>();
    peers.add(this);
    FakeBroadcastChannel.channels.set(name, peers);
    FakeBroadcastChannel.instances.push(this);
  }

  postMessage(message: unknown): void {
    if (this.closed || this.muted) return;
    for (const peer of FakeBroadcastChannel.channels.get(this.name) ?? []) {
      if (peer === this || peer.closed) continue;
      setTimeout(() => {
        if (peer.closed) return;
        for (const listener of [...peer.listeners]) listener({ data: message });
      }, 0);
    }
  }

  addEventListener(_type: 'message', listener: (event: { data: unknown }) => void): void {
    this.listeners.add(listener);
  }

  removeEventListener(_type: 'message', listener: (event: { data: unknown }) => void): void {
    this.listeners.delete(listener);
  }

  close(): void {
    this.closed = true;
    FakeBroadcastChannel.channels.get(this.name)?.delete(this);
  }

  static reset(): void {
    this.channels.clear();
    this.instances = [];
  }
}

class FakeEventSource {
  static instances: FakeEventSource[] = [];
  readonly url: string;
  readonly listeners = new Map<string, Set<(event: any) => void>>();
  readyState = 0;
  closed = false;

  constructor(url: string) {
    this.url = url;
    FakeEventSource.instances.push(this);
  }

  addEventListener(type: string, listener: (event: any) => void): void {
    const set = this.listeners.get(type) ?? new Set<(event: any) => void>();
    set.add(listener);
    this.listeners.set(type, set);
  }

  close(): void {
    this.closed = true;
    this.readyState = 2;
  }

  fire(type: string, data = '', lastEventId?: string): void {
    for (const listener of this.listeners.get(type) ?? []) {
      listener({ type, data, lastEventId });
    }
  }

  fireOpen(): void {
    this.readyState = 1;
    this.fire('open');
  }
}

interface FakeDocument extends ControlDocumentLike {
  setVisibility(next: 'visible' | 'hidden'): void;
}

function fakeDocument(initial: 'visible' | 'hidden' = 'visible'): FakeDocument {
  let visibilityState = initial;
  const listeners = new Set<() => void>();
  return {
    get visibilityState() { return visibilityState; },
    addEventListener(_type, listener) { listeners.add(listener); },
    removeEventListener(_type, listener) { listeners.delete(listener); },
    setVisibility(next) {
      visibilityState = next;
      for (const listener of [...listeners]) listener();
    },
  };
}

function fakeWindow(): ControlWindowLike {
  const listeners = new Map<string, Set<(event?: unknown) => void>>();
  return {
    addEventListener(type, listener) {
      const set = listeners.get(type) ?? new Set<(event?: unknown) => void>();
      set.add(listener);
      listeners.set(type, set);
    },
    removeEventListener(type, listener) {
      listeners.get(type)?.delete(listener);
    },
  };
}

const handles: CrossTabControlStream[] = [];

function makeStream(
  tabId: string,
  doc: FakeDocument = fakeDocument(),
  handlers: Record<string, (data: string, event: MessageEvent) => void> = {},
  overrides: Record<string, unknown> = {},
): CrossTabControlStream {
  const stream = createCrossTabControlStream({
    url: '/api/zero-harness/sse',
    channelKey: 'control:test',
    tabId,
    handlers,
    eventSourceCtor: FakeEventSource as any,
    broadcastChannelCtor: FakeBroadcastChannel as any,
    documentRef: doc,
    windowRef: fakeWindow(),
    electionWindowMs: 50,
    ownerHeartbeatMs: 20,
    ownerLeaseMs: 80,
    visibilityPauseMs: 20,
    zombieTimeoutMs: 0,
    maxConsecutiveFailures: 0,
    ...overrides,
  });
  handles.push(stream);
  return stream;
}

async function settle(): Promise<void> {
  vi.advanceTimersByTime(60);
  await Promise.resolve();
  vi.runOnlyPendingTimers();
  await Promise.resolve();
}

beforeEach(() => {
  vi.useFakeTimers();
  FakeBroadcastChannel.reset();
  FakeEventSource.instances = [];
});

afterEach(() => {
  for (const stream of handles.splice(0)) stream.close();
  FakeBroadcastChannel.reset();
  vi.useRealTimers();
});

describe('cross-tab control stream election', () => {
  it('elects one deterministic owner and keeps one physical EventSource', async () => {
    const a = makeStream('a');
    const b = makeStream('b');
    await settle();

    expect(a.role).toBe('owner');
    expect(a.isOwner).toBe(true);
    expect(b.role).toBe('follower');
    expect(b.ownerTabId).toBe('a');
    expect(FakeEventSource.instances).toHaveLength(1);
    expect(a.coordinationAvailable).toBe(true);
    expect(b.coordinationAvailable).toBe(true);
  });

  it('fans owner payloads and liveness through the existing handler contract', async () => {
    const seen: Array<{ data: string; id: string }> = [];
    const followerOpen = vi.fn();
    const a = makeStream('a', fakeDocument(), {
      update: () => {},
    });
    const b = makeStream('b', fakeDocument(), {
      update: (data, event) => seen.push({ data, id: event.lastEventId }),
    }, { onOpen: followerOpen });
    await settle();

    const physical = FakeEventSource.instances[0]!;
    physical.fireOpen();
    physical.fire('update', '{"name":"x"}', '17');
    vi.runOnlyPendingTimers();
    await Promise.resolve();

    expect(seen).toEqual([{ data: '{"name":"x"}', id: '17' }]);
    expect(b.lastEventId).toBe('17');
    expect(b.status).toBe('open');
    expect(a.status).toBe('open');
    expect(followerOpen).toHaveBeenCalledTimes(1);
  });

  it('releases a hidden owner and lets a visible follower take over', async () => {
    const ownerDoc = fakeDocument();
    const followerDoc = fakeDocument();
    const a = makeStream('a', ownerDoc);
    const b = makeStream('b', followerDoc);
    await settle();
    expect(a.role).toBe('owner');

    ownerDoc.setVisibility('hidden');
    vi.advanceTimersByTime(20);
    await Promise.resolve();
    // Leaving reaches the follower, then its election window expires.
    vi.advanceTimersByTime(60);
    await Promise.resolve();

    expect(a.role).toBe('paused');
    expect(a.isOwner).toBe(false);
    expect(b.role).toBe('owner');
    expect(b.ownerTabId).toBe('b');
    expect(FakeEventSource.instances.filter((source) => !source.closed)).toHaveLength(1);
  });

  it('fails over after the owner lease expires when heartbeats disappear', async () => {
    const a = makeStream('a');
    const b = makeStream('b');
    await settle();
    expect(a.role).toBe('owner');

    // Simulate a dead/unresponsive owner without sending its graceful leaving
    // message. The follower must rely on the lease, not a permanent owner bit.
    const ownerChannel = FakeBroadcastChannel.instances.find((channel) => channel.name?.includes('control:test'))!;
    ownerChannel.muted = true;
    vi.advanceTimersByTime(90);
    await Promise.resolve();
    vi.advanceTimersByTime(60);
    await Promise.resolve();

    expect(b.role).toBe('owner');
    expect(b.ownerTabId).toBe('b');
  });
});

describe('cross-tab control stream fallback and lifecycle', () => {
  it('carries the last event id when a hidden owner recreates its physical source', async () => {
    const doc = fakeDocument();
    const stream = makeStream('a', doc, { invalidate: () => {} });
    await settle();

    FakeEventSource.instances[0]!.fireOpen();
    FakeEventSource.instances[0]!.fire('invalidate', '{"name":"x"}', '17');
    expect(stream.lastEventId).toBe('17');

    doc.setVisibility('hidden');
    vi.advanceTimersByTime(20);
    await Promise.resolve();
    expect(stream.role).toBe('paused');

    doc.setVisibility('visible');
    vi.advanceTimersByTime(60);
    await Promise.resolve();

    expect(stream.role).toBe('owner');
    expect(FakeEventSource.instances).toHaveLength(2);
    expect(new URL(FakeEventSource.instances[1]!.url, 'http://x').searchParams.get('lastEventId')).toBe('17');
    expect(stream.lastEventId).toBe('17');
  });

  it('uses one standalone source per tab when coordination is unavailable', () => {
    const original = (globalThis as any).BroadcastChannel;
    (globalThis as any).BroadcastChannel = undefined;
    try {
      const a = makeStream('a', fakeDocument(), {}, { broadcastChannelCtor: undefined });
      const b = makeStream('b', fakeDocument(), {}, { broadcastChannelCtor: undefined });
      expect(a.coordinationAvailable).toBe(false);
      expect(b.coordinationAvailable).toBe(false);
      expect(a.role).toBe('standalone');
      expect(b.role).toBe('standalone');
      expect(FakeEventSource.instances).toHaveLength(2);
    } finally {
      (globalThis as any).BroadcastChannel = original;
    }
  });

  it('falls back if the BroadcastChannel constructor throws', () => {
    class ThrowingChannel {
      constructor() { throw new Error('BroadcastChannel unavailable'); }
    }
    const stream = makeStream('a', fakeDocument(), {}, {
      broadcastChannelCtor: ThrowingChannel as any,
    });
    expect(stream.coordinationAvailable).toBe(false);
    expect(stream.role).toBe('standalone');
    expect(FakeEventSource.instances).toHaveLength(1);
  });

  it('broadcasts a leaving message before close and becomes permanently closed', async () => {
    const a = makeStream('a');
    const b = makeStream('b');
    await settle();
    expect(a.role).toBe('owner');

    a.close();
    vi.advanceTimersByTime(60);
    await Promise.resolve();
    expect(a.status).toBe('closed');
    expect(a.role).toBe('closed');
    expect(b.role).toBe('owner');
    expect(FakeEventSource.instances.filter((source) => !source.closed)).toHaveLength(1);
  });
});
