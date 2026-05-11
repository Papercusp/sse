import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { getChannel, dropChannel, listChannels, _resetChannelsForTest } from './channel';

beforeEach(() => { _resetChannelsForTest(); });
afterEach(() => { _resetChannelsForTest(); });

async function collect<T>(it: AsyncIterable<T>, count: number, timeoutMs = 1000): Promise<T[]> {
  const out: T[] = [];
  const iter = it[Symbol.asyncIterator]();
  for (let i = 0; i < count; i++) {
    const next = await Promise.race([
      iter.next(),
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error('timeout')), timeoutMs)),
    ]);
    if (next.done) break;
    out.push(next.value as T);
  }
  return out;
}

describe('getChannel — basic publish/subscribe', () => {
  it('assigns monotonic ids and delivers to subscribers in order', async () => {
    const ch = getChannel<string>('test:basic');
    const sub = ch.subscribe();
    ch.publish('a');
    ch.publish('b');
    ch.publish('c');
    const items = await collect(sub, 3);
    expect(items.map((i) => i.event)).toEqual(['a', 'b', 'c']);
    expect(items.map((i) => i.id)).toEqual([1, 2, 3]);
  });

  it('publish() returns the assigned id', () => {
    const ch = getChannel<number>('test:returnid');
    expect(ch.publish(0)).toBe(1);
    expect(ch.publish(0)).toBe(2);
  });

  it('returns the same channel for the same key', () => {
    const a = getChannel<string>('test:same');
    const b = getChannel<string>('test:same');
    a.publish('x');
    expect(b.recent).toHaveLength(1);
  });

  it('returns different channels for different keys', () => {
    const a = getChannel<string>('test:a');
    const b = getChannel<string>('test:b');
    a.publish('x');
    expect(b.recent).toHaveLength(0);
  });
});

describe('getChannel — ring buffer', () => {
  it('keeps the last ringSize events; overwrites oldest', () => {
    const ch = getChannel<number>('test:ring', { ringSize: 3 });
    ch.publish(1); ch.publish(2); ch.publish(3); ch.publish(4); ch.publish(5);
    expect(ch.recent.map((e) => e.event)).toEqual([3, 4, 5]);
    expect(ch.recent.map((e) => e.id)).toEqual([3, 4, 5]);
  });

  it('recentSince filters by id', () => {
    const ch = getChannel<string>('test:since');
    ch.publish('a'); ch.publish('b'); ch.publish('c');
    expect(ch.recentSince(0).map((e) => e.event)).toEqual(['a', 'b', 'c']);
    expect(ch.recentSince(1).map((e) => e.event)).toEqual(['b', 'c']);
    expect(ch.recentSince(3).map((e) => e.event)).toEqual([]);
    expect(ch.recentSince(null).map((e) => e.event)).toEqual(['a', 'b', 'c']);
    expect(ch.recentSince(undefined).map((e) => e.event)).toEqual(['a', 'b', 'c']);
  });
});

describe('getChannel — multi-subscriber fan-out', () => {
  it('delivers every event to every subscriber', async () => {
    const ch = getChannel<string>('test:fanout');
    const a = ch.subscribe();
    const b = ch.subscribe();
    ch.publish('x'); ch.publish('y');
    expect(ch.subscriberCount).toBe(2);
    const aItems = await collect(a, 2);
    const bItems = await collect(b, 2);
    expect(aItems.map((i) => i.event)).toEqual(['x', 'y']);
    expect(bItems.map((i) => i.event)).toEqual(['x', 'y']);
  });

  it('does NOT yield ring buffer to new subscribers — only future events', async () => {
    const ch = getChannel<string>('test:nobackfill');
    ch.publish('past1');
    ch.publish('past2');
    const sub = ch.subscribe();
    ch.publish('future1');
    const items = await collect(sub, 1);
    expect(items.map((i) => i.event)).toEqual(['future1']);
  });

  it('subscriber that breaks out of iteration is cleaned up', async () => {
    const ch = getChannel<number>('test:break');
    const iter = ch.subscribe()[Symbol.asyncIterator]();
    ch.publish(1);
    await iter.next();
    expect(ch.subscriberCount).toBe(1);
    await iter.return!(undefined);
    expect(ch.subscriberCount).toBe(0);
  });
});

describe('getChannel — done()', () => {
  it('done() ends active subscribers iteration', async () => {
    const ch = getChannel<string>('test:done');
    const iter = ch.subscribe()[Symbol.asyncIterator]();
    ch.publish('a');
    const first = await iter.next();
    expect(first.done).toBe(false);
    ch.done({ reason: 'completed' });
    const next = await iter.next();
    expect(next.done).toBe(true);
    expect(ch.isDone).toBe(true);
    expect(ch.donePayload).toEqual({ reason: 'completed' });
  });

  it('done() is idempotent', () => {
    const ch = getChannel<string>('test:idem');
    ch.done('first');
    ch.done('second');
    expect(ch.donePayload).toBe('first');
  });

  it('subscribing after done() yields nothing (immediate end)', async () => {
    const ch = getChannel<string>('test:postdone');
    ch.publish('lost-on-purpose');
    ch.done();
    const iter = ch.subscribe()[Symbol.asyncIterator]();
    const next = await iter.next();
    expect(next.done).toBe(true);
  });

  it('publish() after done() throws', () => {
    const ch = getChannel<string>('test:postdonepub');
    ch.done();
    expect(() => ch.publish('x')).toThrow(/after done/);
  });
});

describe('getChannel — backpressure / queue overflow', () => {
  it('drops oldest events from a subscriber queue when it overflows', async () => {
    const ch = getChannel<number>('test:overflow', { subscriberQueueSize: 3 });
    const iter = ch.subscribe()[Symbol.asyncIterator]();
    for (let i = 1; i <= 5; i++) ch.publish(i);
    // Consume all that's in the queue.
    const items: number[] = [];
    for (let i = 0; i < 3; i++) {
      const v = await iter.next();
      if (v.done) break;
      items.push(v.value.event);
    }
    expect(items).toEqual([3, 4, 5]); // oldest 2 dropped
  });
});

describe('getChannel — GC after done + zero subscribers', () => {
  it('schedules cleanup after gcDelayMs', async () => {
    vi.useFakeTimers();
    const ch = getChannel<string>('test:gc', { gcDelayMs: 1000 });
    const iter = ch.subscribe()[Symbol.asyncIterator]();
    ch.publish('x');
    ch.done();
    await iter.next();
    await iter.next(); // observe done
    expect(listChannels().some((c) => c.key === 'test:gc')).toBe(true);
    vi.advanceTimersByTime(1001);
    expect(listChannels().some((c) => c.key === 'test:gc')).toBe(false);
    vi.useRealTimers();
  });

  it('cancels GC if a new subscribe comes in before the timer fires', async () => {
    vi.useFakeTimers();
    const ch = getChannel<string>('test:gccancel', { gcDelayMs: 1000 });
    const iter = ch.subscribe()[Symbol.asyncIterator]();
    ch.done();
    await iter.next();
    vi.advanceTimersByTime(500);
    // New subscribe — must reuse the channel (and re-open it).
    const ch2 = getChannel<string>('test:gccancel');
    ch2.subscribe(); // touches state
    vi.advanceTimersByTime(2000);
    expect(listChannels().some((c) => c.key === 'test:gccancel')).toBe(true);
    vi.useRealTimers();
  });
});

describe('getChannel — globalThis pinning (HMR survival)', () => {
  it('uses the globalThis-pinned registry', () => {
    const ch = getChannel<string>('test:hmr');
    ch.publish('x');
    const g = globalThis as unknown as { __restartSseChannels__: Map<string, unknown> };
    expect(g.__restartSseChannels__).toBeDefined();
    expect(g.__restartSseChannels__.has('test:hmr')).toBe(true);
  });
});

describe('dropChannel + listChannels', () => {
  it('listChannels returns active channels', () => {
    getChannel<string>('test:listed-a');
    getChannel<string>('test:listed-b');
    const list = listChannels().map((c) => c.key);
    expect(list).toContain('test:listed-a');
    expect(list).toContain('test:listed-b');
  });

  it('dropChannel removes from registry', () => {
    getChannel<string>('test:droppable');
    expect(dropChannel('test:droppable')).toBe(true);
    expect(listChannels().some((c) => c.key === 'test:droppable')).toBe(false);
    expect(dropChannel('test:droppable')).toBe(false); // already gone
  });
});
