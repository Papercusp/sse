/**
 * Last-Event-ID reconnect resume.
 *
 * Models the full production scenario: a channel emits events, a client
 * disconnects mid-stream, the server keeps publishing, the client
 * reconnects sending Last-Event-ID, and the server replays from the ring
 * buffer ONLY the events the client missed (no duplication, no skip).
 *
 * This is the critical behavior that makes desktop sync resilient. If the
 * snapshot wire-format gate ever needs explanation, this is "why".
 */

import { describe, it, expect, afterEach } from 'vitest';
import { createServer, type Server } from 'node:http';
import {
  sseResponse,
  parseSseStream,
  getChannel,
  parseLastEventId,
  bridgeChannel,
  _resetChannelsForTest,
  type SyncSseEventVocabulary,
} from '../index';

interface ParsedItem { event: string; data: string; id?: string }
let activeServer: Server | null = null;
afterEach(async () => {
  _resetChannelsForTest();
  if (activeServer) {
    await new Promise<void>((resolve) => activeServer!.close(() => resolve()));
    activeServer = null;
  }
});

/** Spin up an HTTP server. Routes shared across requests. */
async function listenOn(handler: (req: Request) => Response | Promise<Response>): Promise<{ url: string }> {
  const server = createServer(async (incoming, outgoing) => {
    const url = `http://${incoming.headers.host}${incoming.url ?? ''}`;
    const headers = new Headers();
    for (const [k, v] of Object.entries(incoming.headers)) {
      if (typeof v === 'string') headers.set(k, v);
    }
    const controller = new AbortController();
    incoming.on('close', () => controller.abort());
    const req = new Request(url, { headers, signal: controller.signal });
    const res = await handler(req);
    outgoing.statusCode = res.status;
    res.headers.forEach((value, key) => outgoing.setHeader(key, value));
    if (!res.body) { outgoing.end(); return; }
    const reader = res.body.getReader();
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        if (!outgoing.writable) break;
        outgoing.write(value);
      }
    } finally {
      try { reader.releaseLock(); } catch { /* ignore */ }
      outgoing.end();
    }
  });
  activeServer = server;
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  const addr = server.address();
  if (typeof addr !== 'object' || addr === null) throw new Error('no addr');
  return { url: `http://127.0.0.1:${addr.port}` };
}

describe('Last-Event-ID reconnect resume', () => {
  it('client missed events are replayed in order with no gap and no duplication', async () => {
    type V = SyncSseEventVocabulary<{ n: number }>;
    const ch = getChannel<{ name: 'tick'; data: { n: number } }>('test:lei');

    // Server: subscribers see live + replay from channel.recentSince(LEI).
    const { url } = await listenOn((req) => {
      const lei = parseLastEventId(req);
      return sseResponse<V>({
        signal: req.signal,
        lastEventId: lei,
        heartbeatMs: 0,
        initialHeartbeat: false,
        replay: () => ch.recentSince(lei ?? 0).map((item) => ({
          id: item.id,
          name: 'update' as const,
          data: { name: 'tick', args: undefined, data: item.event.data, tsMs: 0 },
        })),
        setup: (sink) => bridgeChannel(ch.subscribe(), sink, ({ id, event }) => ({
          name: 'update',
          data: { name: event.name, data: event.data, tsMs: 0 },
          opts: { id },
        })),
      });
    });

    /** Read `n` events of the given kind from an SSE response, then abort. */
    async function readN(req: { url: string; lastEventId?: number | null }, kind: string, n: number) {
      const ac = new AbortController();
      const res = await fetch(req.url, {
        headers: req.lastEventId != null ? { 'Last-Event-ID': String(req.lastEventId) } : {},
        signal: ac.signal,
      });
      const got: ParsedItem[] = [];
      let maxId = req.lastEventId ?? 0;
      try {
        for await (const ev of parseSseStream(res.body)) {
          if (ev.event === kind) {
            got.push({ event: ev.event, data: ev.data, id: ev.id });
            if (ev.id) maxId = Math.max(maxId, Number(ev.id));
            if (got.length >= n) break;
          }
        }
      } finally {
        ac.abort();
      }
      return { got, maxId };
    }

    // 1. Publish 3 events.
    ch.publish({ name: 'tick', data: { n: 1 } });
    ch.publish({ name: 'tick', data: { n: 2 } });
    ch.publish({ name: 'tick', data: { n: 3 } });

    // 2. Client reads 3 updates, then aborts.
    const first = await readN({ url }, 'update', 3);
    expect(first.got.map((e) => JSON.parse(e.data).data.n)).toEqual([1, 2, 3]);
    expect(first.maxId).toBe(3);

    // Give the server a tick to process the abort + unsubscribe.
    await new Promise((r) => setTimeout(r, 50));

    // 3. Publish 4 more events.
    ch.publish({ name: 'tick', data: { n: 4 } });
    ch.publish({ name: 'tick', data: { n: 5 } });
    ch.publish({ name: 'tick', data: { n: 6 } });
    ch.publish({ name: 'tick', data: { n: 7 } });

    // 4. Reconnect with Last-Event-ID, read 4 more.
    const second = await readN({ url, lastEventId: first.maxId }, 'update', 4);
    expect(second.got.map((e) => JSON.parse(e.data).data.n)).toEqual([4, 5, 6, 7]);
    expect(second.maxId).toBe(7);

    ch.done();
  });

  it('reconnect with LEI > current max yields no events', async () => {
    type V = SyncSseEventVocabulary<{ n: number }>;
    const ch = getChannel<{ name: 'tick'; data: { n: number } }>('test:lei-future');

    const { url } = await listenOn((req) => {
      const lei = parseLastEventId(req);
      return sseResponse<V>({
        signal: req.signal,
        lastEventId: lei,
        heartbeatMs: 0,
        initialHeartbeat: false,
        replay: () => ch.recentSince(lei ?? 0).map((item) => ({
          id: item.id,
          name: 'update' as const,
          data: { name: 'tick', args: undefined, data: item.event.data, tsMs: 0 },
        })),
        setup: (sink) => sink.done({ reason: 'no-live-needed' }),
      });
    });

    ch.publish({ name: 'tick', data: { n: 1 } });
    ch.publish({ name: 'tick', data: { n: 2 } });

    // Client claims to have seen up to id=999 (e.g. operator restarted; client
    // is from before the restart, so its id is past server's current max).
    const res = await fetch(url, { headers: { 'Last-Event-ID': '999' } });
    const events: string[] = [];
    for await (const ev of parseSseStream(res.body)) events.push(ev.event);

    // Should see only `done` (no replay; setup ends immediately).
    expect(events).toEqual(['done']);
  });
});
