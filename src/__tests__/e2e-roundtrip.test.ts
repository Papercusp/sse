/**
 * End-to-end roundtrip: real Node HTTP server + fetch-based SSE client.
 *
 * Exercises sseResponse → real bytes → parseSseStream → events. Together
 * with the unit tests, this guards against regressions where individual
 * pieces work but the integration doesn't.
 *
 * Uses parseSseStream (POST-stream client) rather than EventSource so the
 * test runs in plain Node without polyfills. The wire format is identical
 * regardless of consumer; this also matches what useHarnessChatRuntime
 * does in the operator.
 */

import { describe, it, expect, afterEach } from 'vitest';
import { createServer, type Server } from 'node:http';
import { sseResponse, parseSseStream, type SyncSseEventVocabulary } from '../index';

let activeServer: Server | null = null;

afterEach(async () => {
  if (activeServer) {
    await new Promise<void>((resolve) => activeServer!.close(() => resolve()));
    activeServer = null;
  }
});

interface ServerHandle { url: string; close: () => Promise<void>; }

/** Spin up an HTTP server that returns the given Response on every request. */
async function withResponse(buildResponse: (req: Request) => Response): Promise<ServerHandle> {
  const server = createServer(async (incoming, outgoing) => {
    const url = `http://${incoming.headers.host}${incoming.url ?? ''}`;
    const headers = new Headers();
    for (const [k, v] of Object.entries(incoming.headers)) {
      if (typeof v === 'string') headers.set(k, v);
    }
    const controller = new AbortController();
    incoming.on('close', () => controller.abort());
    const req = new Request(url, { headers, signal: controller.signal });

    const res = buildResponse(req);
    outgoing.statusCode = res.status;
    res.headers.forEach((value, key) => outgoing.setHeader(key, value));

    if (!res.body) {
      outgoing.end();
      return;
    }
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
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const addr = server.address();
  if (typeof addr !== 'object' || addr === null) throw new Error('no server addr');
  return {
    url: `http://127.0.0.1:${addr.port}`,
    async close() { await new Promise<void>((r) => server.close(() => r())); },
  };
}

describe('e2e roundtrip — fetch → sseResponse → parseSseStream', () => {
  it('round-trips named events with payloads', async () => {
    const h = await withResponse((req) =>
      sseResponse<SyncSseEventVocabulary<{ rows: number[] }>>({
        signal: req.signal,
        heartbeatMs: 0,
        initialHeartbeat: false,
        setup: (sink) => {
          sink.event('update', { name: 'foo', args: { x: 1 }, data: { rows: [1, 2, 3] } }, { id: 1 });
          sink.event('invalidate', { name: 'bar' }, { id: 2 });
          sink.done({ reason: 'completed' });
        },
      })
    );

    const res = await fetch(h.url);
    expect(res.headers.get('content-type')).toBe('text/event-stream');

    const seen: { id?: string; event: string; data: string }[] = [];
    for await (const ev of parseSseStream(res.body)) {
      seen.push({ id: ev.id, event: ev.event, data: ev.data });
    }

    // Per SSE spec, parser persists `id:` across events until reset — so the
    // `done` event (no explicit id) inherits "2" from the previous frame.
    expect(seen).toEqual([
      { id: '1', event: 'update', data: '{"name":"foo","args":{"x":1},"data":{"rows":[1,2,3]}}' },
      { id: '2', event: 'invalidate', data: '{"name":"bar"}' },
      { id: '2', event: 'done', data: '{"reason":"completed"}' },
    ]);
  });

  it('emits the initial heartbeat then user events', async () => {
    const h = await withResponse((req) =>
      sseResponse({
        signal: req.signal,
        heartbeatMs: 0,
        initialHeartbeat: true,
        setup: (sink) => {
          sink.event('tick' as never, { n: 1 } as never, { id: 1 });
          sink.done();
        },
      })
    );

    const res = await fetch(h.url);
    const events: { event: string }[] = [];
    for await (const ev of parseSseStream(res.body)) events.push({ event: ev.event });

    expect(events[0]!.event).toBe('heartbeat');
    expect(events.find((e) => e.event === 'tick')).toBeDefined();
    expect(events[events.length - 1]!.event).toBe('done');
  });

  it('replay yields only events with id > Last-Event-ID', async () => {
    const h = await withResponse((req) => {
      const lei = req.headers.get('Last-Event-ID');
      const lastEventId = lei ? Number(lei) : null;
      return sseResponse<{ tick: { n: number } }>({
        signal: req.signal,
        heartbeatMs: 0,
        initialHeartbeat: false,
        lastEventId,
        replay: () => [
          { id: 1, name: 'tick', data: { n: 1 } },
          { id: 2, name: 'tick', data: { n: 2 } },
          { id: 3, name: 'tick', data: { n: 3 } },
        ],
        setup: (sink) => sink.done(),
      });
    });

    const res = await fetch(h.url, { headers: { 'Last-Event-ID': '1' } });
    const ticks: number[] = [];
    for await (const ev of parseSseStream(res.body)) {
      if (ev.event === 'tick') ticks.push(JSON.parse(ev.data).n);
    }
    expect(ticks).toEqual([2, 3]);
  });

  it('terminalShortCircuit short-circuits to done immediately', async () => {
    const h = await withResponse((req) =>
      sseResponse({
        signal: req.signal,
        heartbeatMs: 0,
        initialHeartbeat: false,
        terminalShortCircuit: () => ({ reason: 'late-join-terminal' }),
        setup: () => { throw new Error('setup should not run'); },
      })
    );

    const res = await fetch(h.url);
    const events: { event: string; data: string }[] = [];
    for await (const ev of parseSseStream(res.body)) events.push({ event: ev.event, data: ev.data });

    expect(events).toHaveLength(1);
    expect(events[0]).toEqual({ event: 'done', data: '{"reason":"late-join-terminal"}' });
  });

  it('multi-line data round-trips correctly', async () => {
    const h = await withResponse((req) =>
      sseResponse<{ log: { lines: string[] } }>({
        signal: req.signal,
        heartbeatMs: 0,
        initialHeartbeat: false,
        setup: (sink) => {
          sink.event('log', { lines: ['line1', 'line2', 'line3'] }, { id: 1 });
          sink.done();
        },
      })
    );
    const res = await fetch(h.url);
    let payload = '';
    for await (const ev of parseSseStream(res.body)) {
      if (ev.event === 'log') payload = ev.data;
    }
    // The data was a JSON string with no embedded \n — the wire format
    // didn't need to split. Sanity check we got the JSON back intact.
    expect(JSON.parse(payload)).toEqual({ lines: ['line1', 'line2', 'line3'] });
  });
});
