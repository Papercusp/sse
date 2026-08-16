import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { pgListenChannel } from './pg-listen-channel';
import { _resetChannelsForTest } from './channel';

beforeEach(() => { _resetChannelsForTest(); });
afterEach(() => { _resetChannelsForTest(); });

/**
 * Fake postgres-js Sql client. Captures `sql.listen()` callbacks so the
 * test can invoke them directly. `unlisten()` resolves and flips a flag.
 */
function makeFakeSql() {
  const handlers = new Map<string, (payload: string) => void>();
  const unlistens = new Map<string, boolean>();
  const fake = {
    async listen(channel: string, cb: (payload: string) => void) {
      handlers.set(channel, cb);
      unlistens.set(channel, false);
      return {
        unlisten: async () => {
          unlistens.set(channel, true);
        },
      };
    },
  };
  return {
    sql: fake as unknown as import('postgres').Sql,
    notify(channel: string, payload: string) {
      handlers.get(channel)?.(payload);
    },
    isUnlistened(channel: string) {
      return unlistens.get(channel) ?? false;
    },
  };
}

async function collect<T>(it: AsyncIterable<T>, count: number): Promise<T[]> {
  const out: T[] = [];
  const iter = it[Symbol.asyncIterator]();
  for (let i = 0; i < count; i++) {
    const next = await Promise.race([
      iter.next(),
      new Promise<never>((_, r) => setTimeout(() => r(new Error('timeout')), 1000)),
    ]);
    if (next.done) break;
    out.push(next.value as T);
  }
  return out;
}

describe('pgListenChannel', () => {
  it('parses and publishes each NOTIFY to subscribers', async () => {
    const f = makeFakeSql();
    const ch = await pgListenChannel<{ x: number }>({
      sql: f.sql,
      channel: 'test_chan',
    });
    const sub = ch.subscribe();
    f.notify('test_chan', '{"x":1}');
    f.notify('test_chan', '{"x":2}');
    f.notify('test_chan', '{"x":3}');
    const items = await collect(sub, 3);
    expect(items.map((i) => i.event)).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);
    expect(items.map((i) => i.id)).toEqual([1, 2, 3]);
  });

  it('uses a custom parse function when provided', async () => {
    const f = makeFakeSql();
    const ch = await pgListenChannel<string>({
      sql: f.sql,
      channel: 'raw',
      parse: (s) => s.toUpperCase(),
    });
    const sub = ch.subscribe();
    f.notify('raw', 'hello');
    const [item] = await collect(sub, 1);
    expect(item!.event).toBe('HELLO');
  });

  it('swallows parse errors and continues listening', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const f = makeFakeSql();
    const ch = await pgListenChannel<{ ok: boolean }>({
      sql: f.sql,
      channel: 'bad',
    });
    const sub = ch.subscribe();
    f.notify('bad', 'not json');
    f.notify('bad', '{"ok":true}');
    const items = await collect(sub, 1);
    expect(items.map((i) => i.event)).toEqual([{ ok: true }]);
    expect(warnSpy).toHaveBeenCalled();
    warnSpy.mockRestore();
  });

  it('recentSince filters by id (in-memory ring)', async () => {
    const f = makeFakeSql();
    const ch = await pgListenChannel<number>({
      sql: f.sql,
      channel: 'nums',
      parse: (s) => Number(s),
    });
    f.notify('nums', '10');
    f.notify('nums', '20');
    f.notify('nums', '30');
    expect(ch.recentSince(0).map((e) => e.event)).toEqual([10, 20, 30]);
    expect(ch.recentSince(1).map((e) => e.event)).toEqual([20, 30]);
    expect(ch.recentSince(2).map((e) => e.event)).toEqual([30]);
    expect(ch.recentSince(3)).toEqual([]);
  });

  it('backfillSince calls the backfill callback', async () => {
    const f = makeFakeSql();
    const backfill = vi.fn(async (sinceId: number) => [
      { id: 100, event: { v: 'a' } },
      { id: 101, event: { v: 'b' } },
    ].filter((e) => e.id > sinceId));
    const ch = await pgListenChannel<{ v: string }>({
      sql: f.sql,
      channel: 'bf',
      backfill,
    });
    const items = await ch.backfillSince(99);
    expect(items).toEqual([
      { id: 100, event: { v: 'a' } },
      { id: 101, event: { v: 'b' } },
    ]);
    expect(backfill).toHaveBeenCalledWith(99);
  });

  it('backfillSince returns [] when no backfill is configured', async () => {
    const f = makeFakeSql();
    const ch = await pgListenChannel<number>({
      sql: f.sql,
      channel: 'no-bf',
    });
    expect(await ch.backfillSince(0)).toEqual([]);
  });

  it('backfillSince swallows errors and returns []', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const f = makeFakeSql();
    const ch = await pgListenChannel<number>({
      sql: f.sql,
      channel: 'bf-err',
      backfill: async () => { throw new Error('db blew up'); },
    });
    expect(await ch.backfillSince(0)).toEqual([]);
    expect(warnSpy).toHaveBeenCalled();
    warnSpy.mockRestore();
  });

  it('unlisten() stops listening and marks the channel done', async () => {
    const f = makeFakeSql();
    const ch = await pgListenChannel<number>({
      sql: f.sql,
      channel: 'stop',
      parse: (s) => Number(s),
    });
    expect(f.isUnlistened('stop')).toBe(false);
    await ch.unlisten();
    expect(f.isUnlistened('stop')).toBe(true);
    expect(ch.isDone).toBe(true);
    expect(ch.donePayload).toEqual({ reason: 'unlisten' });
  });

  it('unlisten() is idempotent', async () => {
    const f = makeFakeSql();
    const ch = await pgListenChannel<number>({
      sql: f.sql,
      channel: 'idem',
    });
    await ch.unlisten();
    await ch.unlisten();
    expect(ch.isDone).toBe(true);
  });

  it('uses the custom channelKey when provided', async () => {
    const f = makeFakeSql();
    const ch = await pgListenChannel<number>({
      sql: f.sql,
      channel: 'pg_chan',
      channelKey: 'my-domain:thing',
      parse: (s) => Number(s),
    });
    f.notify('pg_chan', '5');
    // recent is the only post-facto observable for the channel key here.
    expect(ch.recent.map((e) => e.event)).toEqual([5]);
  });

  it('respects ringSize override', async () => {
    const f = makeFakeSql();
    const ch = await pgListenChannel<number>({
      sql: f.sql,
      channel: 'small-ring',
      parse: (s) => Number(s),
      ringSize: 2,
    });
    f.notify('small-ring', '1');
    f.notify('small-ring', '2');
    f.notify('small-ring', '3');
    expect(ch.recent.map((e) => e.event)).toEqual([2, 3]);
  });
});
