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
    const g = globalThis as unknown as { __sse_channel_registry__: Map<string, unknown> };
    expect(g.__sse_channel_registry__).toBeDefined();
    expect(g.__sse_channel_registry__.has('test:hmr')).toBe(true);
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

  it('listChannels exposes syncHandlers count', () => {
    const ch = getChannel<string>('test:sync-count');
    expect(listChannels().find((c) => c.key === 'test:sync-count')?.syncHandlers).toBe(0);
    const off = ch.onPublish(() => {});
    expect(listChannels().find((c) => c.key === 'test:sync-count')?.syncHandlers).toBe(1);
    off();
    expect(listChannels().find((c) => c.key === 'test:sync-count')?.syncHandlers).toBe(0);
  });

  it('dropChannel removes from registry', () => {
    getChannel<string>('test:droppable');
    expect(dropChannel('test:droppable')).toBe(true);
    expect(listChannels().some((c) => c.key === 'test:droppable')).toBe(false);
    expect(dropChannel('test:droppable')).toBe(false); // already gone
  });
});

describe('onPublish + onDone (synchronous handlers)', () => {
  it('onPublish handler fires synchronously inside publish, in publish order', () => {
    const ch = getChannel<string>('test:onpub-order');
    const seen: string[] = [];
    ch.onPublish((item) => seen.push(item.event));
    ch.publish('a');
    expect(seen).toEqual(['a']);
    ch.publish('b');
    ch.publish('c');
    expect(seen).toEqual(['a', 'b', 'c']);
  });

  it('onPublish unsubscribe stops further delivery', () => {
    const ch = getChannel<string>('test:onpub-unsub');
    const seen: string[] = [];
    const off = ch.onPublish((item) => seen.push(item.event));
    ch.publish('a');
    off();
    ch.publish('b');
    expect(seen).toEqual(['a']);
  });

  it('onPublish handler throws are swallowed; siblings + producer continue', () => {
    const ch = getChannel<string>('test:onpub-throw');
    const seen: string[] = [];
    ch.onPublish(() => { throw new Error('boom'); });
    ch.onPublish((item) => seen.push(item.event));
    expect(() => ch.publish('a')).not.toThrow();
    expect(seen).toEqual(['a']);
  });

  it('multiple onPublish handlers fan-out to each in registration order', () => {
    const ch = getChannel<string>('test:onpub-fanout');
    const a: string[] = [];
    const b: string[] = [];
    ch.onPublish((item) => a.push(item.event));
    ch.onPublish((item) => b.push(item.event));
    ch.publish('x');
    expect(a).toEqual(['x']);
    expect(b).toEqual(['x']);
  });

  it('onDone fires when channel transitions to done', () => {
    const ch = getChannel<string>('test:ondone-trans');
    let fired = false;
    ch.onDone(() => { fired = true; });
    expect(fired).toBe(false);
    ch.done();
    expect(fired).toBe(true);
  });

  it('onDone fires immediately if channel is already done', () => {
    const ch = getChannel<string>('test:ondone-already');
    ch.done();
    let fired = false;
    ch.onDone(() => { fired = true; });
    expect(fired).toBe(true);
  });

  it('onDone unsubscribe prevents fire on subsequent done() (registered before done)', () => {
    const ch = getChannel<string>('test:ondone-unsub');
    let fired = false;
    const off = ch.onDone(() => { fired = true; });
    off();
    ch.done();
    expect(fired).toBe(false);
  });

  it('sibling-unsubscribe during fan-out: deleted handler does not receive current event', () => {
    const ch = getChannel<string>('test:onpub-sibling-unsub');
    const seen: string[] = [];
    let offB: (() => void) | null = null;
    ch.onPublish((item) => {
      seen.push(`A:${item.event}`);
      if (item.event === 'x') offB!();
    });
    offB = ch.onPublish((item) => seen.push(`B:${item.event}`));
    ch.publish('x'); // A runs first, unsubscribes B before B's slot
    ch.publish('y');
    // B was unsubscribed before its callback fired for 'x', so it shouldn't
    // see anything.
    expect(seen).toEqual(['A:x', 'A:y']);
  });

  it('self-unsubscribe inside handler: stops receiving from the NEXT publish onwards', () => {
    const ch = getChannel<string>('test:onpub-self-unsub');
    const seen: string[] = [];
    let off: (() => void) | null = null;
    off = ch.onPublish((item) => {
      seen.push(`A:${item.event}`);
      if (item.event === 'b') off!();
    });
    ch.onPublish((item) => seen.push(`B:${item.event}`));
    ch.publish('a');
    ch.publish('b'); // A unsubscribes itself here
    ch.publish('c'); // A should NOT see this
    expect(seen).toEqual(['A:a', 'B:a', 'A:b', 'B:b', 'B:c']);
  });

  it('re-entrant publish inside handler: delivered to all handlers depth-first', () => {
    const ch = getChannel<string>('test:onpub-reentrant');
    const seen: string[] = [];
    let count = 0;
    ch.onPublish((item) => {
      seen.push(item.event);
      if (count++ < 2) ch.publish(`echo-${item.event}`);
    });
    ch.publish('a');
    // The original publish('a') runs the handler, which calls publish('echo-a'),
    // which recurses into the handler again before the outer publish returns.
    expect(seen).toEqual(['a', 'echo-a', 'echo-echo-a']);
  });

  it('syncHandlerCount reflects active registrations', () => {
    const ch = getChannel<string>('test:onpub-count');
    expect(ch.syncHandlerCount).toBe(0);
    const off1 = ch.onPublish(() => {});
    const off2 = ch.onPublish(() => {});
    expect(ch.syncHandlerCount).toBe(2);
    off1();
    expect(ch.syncHandlerCount).toBe(1);
    off2();
    expect(ch.syncHandlerCount).toBe(0);
  });
});
