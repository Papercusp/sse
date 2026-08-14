/**
 * resumeBounds → `resync` control event (agent-tool-delta-protocol Lane D, P-008).
 *
 * The ring buffer evicts oldest at `ringSize`, and `recentSince` silently returns
 * only the retained tail — so a client reconnecting with a `Last-Event-ID` below
 * the buffer floor (events evicted) or above the source max (restart / id reset)
 * would resume from an unrecoverable baseline and silently diverge. With
 * `resumeBounds` wired, `sseResponse` detects this and emits a standardized
 * `resync` event (and skips partial replay) so the client refetches the full
 * snapshot. This pins that fallback-to-full behaviour.
 */
import { describe, it, expect, afterEach } from 'vitest';
import {
  sseResponse,
  parseSseStream,
  getChannel,
  _resetChannelsForTest,
  type BusChannel,
  type SyncSseEventVocabulary,
} from '../index';

afterEach(() => _resetChannelsForTest());

type V = SyncSseEventVocabulary<{ n: number }>;
type Tick = { name: 'tick'; data: { n: number } };

/** Open a resuming SSE response (resumeBounds wired), then end immediately — we
 *  only assert the open-time replay/resync frames. */
function openResume(ch: BusChannel<Tick>, lei: number | null): Response {
  return sseResponse<V>({
    signal: new AbortController().signal,
    lastEventId: lei,
    heartbeatMs: 0,
    initialHeartbeat: false,
    resumeBounds: () => {
      const r = ch.recent;
      return r.length ? { floorId: r[0].id, maxId: r[r.length - 1].id } : null;
    },
    replay: () =>
      ch.recentSince(lei ?? 0).map((item) => ({
        id: item.id,
        name: 'update' as const,
        data: { name: 'tick', data: item.event.data, tsMs: 0 },
      })),
    setup: (sink) => sink.done({ reason: 'done' }),
  });
}

async function collect(res: Response): Promise<Array<{ event: string; data: string }>> {
  const out: Array<{ event: string; data: string }> = [];
  for await (const ev of parseSseStream(res.body!)) out.push({ event: ev.event, data: ev.data });
  return out;
}

describe('sseResponse resumeBounds → resync', () => {
  it('emits resync(gap) + skips replay when the resume point was evicted from the buffer', async () => {
    const ch = getChannel<Tick>('test:resync-gap', { ringSize: 3 });
    for (let n = 1; n <= 6; n++) ch.publish({ name: 'tick', data: { n } }); // ids 1..6; ring retains 4,5,6
    const events = await collect(openResume(ch, 1)); // client last saw id 1 — ids 2,3 evicted (floor 4)

    const resync = events.find((e) => e.event === 'resync');
    expect(resync).toBeDefined();
    expect(JSON.parse(resync!.data)).toMatchObject({ reason: 'gap', fromId: 1, floorId: 4, maxId: 6 });
    expect(events.some((e) => e.event === 'update')).toBe(false); // partial replay skipped
    expect(events.some((e) => e.event === 'done')).toBe(true);
  });

  it('emits resync(ahead) when the client id is past the source max (restart / id reset)', async () => {
    const ch = getChannel<Tick>('test:resync-ahead', { ringSize: 10 });
    ch.publish({ name: 'tick', data: { n: 1 } });
    ch.publish({ name: 'tick', data: { n: 2 } }); // maxId 2
    const events = await collect(openResume(ch, 999)); // client from a dead generation

    const resync = events.find((e) => e.event === 'resync');
    expect(JSON.parse(resync!.data)).toMatchObject({ reason: 'ahead', fromId: 999, maxId: 2 });
    expect(events.some((e) => e.event === 'update')).toBe(false);
  });

  it('does NOT resync when the resume point is still within the buffer (normal replay)', async () => {
    const ch = getChannel<Tick>('test:resync-ok', { ringSize: 10 });
    for (let n = 1; n <= 5; n++) ch.publish({ name: 'tick', data: { n } }); // ids 1..5 all retained
    const events = await collect(openResume(ch, 2)); // floor 1, max 5 — lei 2 recoverable

    expect(events.some((e) => e.event === 'resync')).toBe(false);
    const updates = events.filter((e) => e.event === 'update').map((e) => JSON.parse(e.data).data.n);
    expect(updates).toEqual([3, 4, 5]);
  });

  it('does not resync a fresh client (no Last-Event-ID)', async () => {
    const ch = getChannel<Tick>('test:resync-fresh', { ringSize: 10 });
    ch.publish({ name: 'tick', data: { n: 1 } });
    const events = await collect(openResume(ch, null));
    expect(events.some((e) => e.event === 'resync')).toBe(false);
  });

  it('no resumeBounds wired ⇒ legacy behaviour (no resync even past the floor)', async () => {
    const ch = getChannel<Tick>('test:resync-legacy', { ringSize: 3 });
    for (let n = 1; n <= 6; n++) ch.publish({ name: 'tick', data: { n } });
    const res = sseResponse<V>({
      signal: new AbortController().signal,
      lastEventId: 1,
      heartbeatMs: 0,
      initialHeartbeat: false,
      // resumeBounds intentionally omitted
      replay: () =>
        ch.recentSince(1).map((item) => ({ id: item.id, name: 'update' as const, data: { name: 'tick', data: item.event.data, tsMs: 0 } })),
      setup: (sink) => sink.done({ reason: 'done' }),
    });
    const events = await collect(res);
    expect(events.some((e) => e.event === 'resync')).toBe(false);
    expect(events.some((e) => e.event === 'update')).toBe(true); // legacy partial replay still happens
  });
});
