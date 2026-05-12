import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { sseResponse, parseLastEventId } from './response';

async function readAll(res: Response): Promise<string> {
  const reader = res.body!.getReader();
  const dec = new TextDecoder();
  let out = '';
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    out += dec.decode(value, { stream: true });
  }
  return out;
}

function makeRequest(headers: Record<string, string> = {}): { req: Request; abort: () => void } {
  const controller = new AbortController();
  const req = new Request('http://localhost/x', { headers, signal: controller.signal });
  return { req, abort: () => controller.abort() };
}

describe('parseLastEventId', () => {
  it('parses a numeric Last-Event-ID', () => {
    const { req } = makeRequest({ 'Last-Event-ID': '42' });
    expect(parseLastEventId(req)).toBe(42);
  });
  it('returns null when absent', () => {
    const { req } = makeRequest();
    expect(parseLastEventId(req)).toBeNull();
  });
  it('returns null for non-numeric', () => {
    const { req } = makeRequest({ 'Last-Event-ID': 'abc' });
    expect(parseLastEventId(req)).toBeNull();
  });
});

describe('sseResponse — headers', () => {
  it('sets the four standard headers', async () => {
    const { req } = makeRequest();
    const res = sseResponse({
      signal: req.signal,
      heartbeatMs: 0,
      initialHeartbeat: false,
      setup: (sink) => sink.done(),
    });
    expect(res.headers.get('content-type')).toBe('text/event-stream');
    expect(res.headers.get('cache-control')).toBe('no-cache, no-transform');
    expect(res.headers.get('connection')).toBe('keep-alive');
    expect(res.headers.get('x-accel-buffering')).toBe('no');
    await readAll(res);
  });

  it('allows header override', async () => {
    const { req } = makeRequest();
    const res = sseResponse({
      signal: req.signal,
      heartbeatMs: 0,
      initialHeartbeat: false,
      headers: { 'X-Custom': 'yes' },
      setup: (sink) => sink.done(),
    });
    expect(res.headers.get('x-custom')).toBe('yes');
    await readAll(res);
  });
});

describe('sseResponse — initial heartbeat', () => {
  it('emits a heartbeat frame at open by default', async () => {
    const { req } = makeRequest();
    const res = sseResponse({
      signal: req.signal,
      heartbeatMs: 0,
      setup: (sink) => sink.done(),
    });
    const body = await readAll(res);
    expect(body).toMatch(/^event: heartbeat\ndata: \{"tsMs":\d+\}\n\n/);
  });

  it('suppresses initial heartbeat when initialHeartbeat=false', async () => {
    const { req } = makeRequest();
    const res = sseResponse({
      signal: req.signal,
      heartbeatMs: 0,
      initialHeartbeat: false,
      setup: (sink) => sink.done(),
    });
    const body = await readAll(res);
    expect(body.startsWith('event: heartbeat')).toBe(false);
  });
});

describe('sseResponse — terminalShortCircuit', () => {
  it('emits done immediately and closes when source is already terminal', async () => {
    const { req } = makeRequest();
    const res = sseResponse({
      signal: req.signal,
      heartbeatMs: 0,
      initialHeartbeat: false,
      terminalShortCircuit: () => ({ reason: 'late-join-terminal' }),
      setup: () => { throw new Error('setup should not run after short-circuit'); },
    });
    const body = await readAll(res);
    expect(body).toBe('event: done\ndata: {"reason":"late-join-terminal"}\n\n');
  });

  it('runs setup normally when terminalShortCircuit returns null', async () => {
    const { req } = makeRequest();
    const res = sseResponse<{ x: { v: number } }>({
      signal: req.signal,
      heartbeatMs: 0,
      initialHeartbeat: false,
      terminalShortCircuit: () => null,
      setup: (sink) => {
        sink.event('x', { v: 1 });
        sink.done();
      },
    });
    const body = await readAll(res);
    expect(body).toContain('event: x');
    expect(body).toContain('data: {"v":1}');
  });
});

describe('sseResponse — replay + lastEventId filtering', () => {
  it('replays events with id > lastEventId', async () => {
    const { req } = makeRequest({ 'Last-Event-ID': '2' });
    const res = sseResponse<{ tick: { n: number } }>({
      signal: req.signal,
      heartbeatMs: 0,
      initialHeartbeat: false,
      lastEventId: parseLastEventId(req),
      replay: () => [
        { id: 1, name: 'tick', data: { n: 1 } },
        { id: 2, name: 'tick', data: { n: 2 } },
        { id: 3, name: 'tick', data: { n: 3 } },
        { id: 4, name: 'tick', data: { n: 4 } },
      ],
      setup: (sink) => sink.done(),
    });
    const body = await readAll(res);
    expect(body).toContain('data: {"n":3}');
    expect(body).toContain('data: {"n":4}');
    expect(body).not.toContain('data: {"n":1}');
    expect(body).not.toContain('data: {"n":2}');
  });

  it('advances the auto-id past replay so post-replay events do not collide', async () => {
    const { req } = makeRequest();
    const res = sseResponse<{ a: { v: number }; b: { v: number } }>({
      signal: req.signal,
      heartbeatMs: 0,
      initialHeartbeat: false,
      replay: () => [{ id: 100, name: 'a', data: { v: 1 } }],
      setup: (sink) => {
        sink.event('b', { v: 2 });
        sink.done();
      },
    });
    const body = await readAll(res);
    expect(body).toContain('id: 100\nevent: a\n');
    expect(body).toContain('id: 101\nevent: b\n');
  });
});

describe('sseResponse — sink.event auto-id', () => {
  it('assigns sequential ids starting at lastEventId+1', async () => {
    const { req } = makeRequest({ 'Last-Event-ID': '10' });
    const res = sseResponse<{ x: { v: number } }>({
      signal: req.signal,
      heartbeatMs: 0,
      initialHeartbeat: false,
      lastEventId: parseLastEventId(req),
      setup: (sink) => {
        sink.event('x', { v: 1 });
        sink.event('x', { v: 2 });
        sink.done();
      },
    });
    const body = await readAll(res);
    expect(body).toContain('id: 11\nevent: x\ndata: {"v":1}');
    expect(body).toContain('id: 12\nevent: x\ndata: {"v":2}');
  });

  it('honors explicit ids on sink.event', async () => {
    const { req } = makeRequest();
    const res = sseResponse<{ x: { v: number } }>({
      signal: req.signal,
      heartbeatMs: 0,
      initialHeartbeat: false,
      setup: (sink) => {
        sink.event('x', { v: 1 }, { id: 99 });
        sink.done();
      },
    });
    const body = await readAll(res);
    expect(body).toContain('id: 99\nevent: x\n');
  });
});

describe('sseResponse — onClose handler', () => {
  it('runs onClose on sink.done()', async () => {
    const { req } = makeRequest();
    const cleanup = vi.fn();
    const res = sseResponse({
      signal: req.signal,
      heartbeatMs: 0,
      initialHeartbeat: false,
      setup: (sink) => {
        sink.onClose(cleanup);
        sink.done();
      },
    });
    await readAll(res);
    expect(cleanup).toHaveBeenCalledTimes(1);
  });

  it('runs onClose on signal abort', async () => {
    const { req, abort } = makeRequest();
    const cleanup = vi.fn();
    const res = sseResponse({
      signal: req.signal,
      heartbeatMs: 0,
      initialHeartbeat: true,                   // ensures reader.read() returns promptly
      setup: (sink) => sink.onClose(cleanup),
    });
    const reader = res.body!.getReader();
    await reader.read(); // returns the initial heartbeat
    abort();
    // Drain until stream closes so we know runClose has fully fired.
    try { while (!(await reader.read()).done) { /* drain */ } } catch { /* ignore */ }
    expect(cleanup).toHaveBeenCalledTimes(1);
  });

  it('does not double-fire onClose on done + close', async () => {
    const { req } = makeRequest();
    const cleanup = vi.fn();
    const res = sseResponse({
      signal: req.signal,
      heartbeatMs: 0,
      initialHeartbeat: false,
      setup: (sink) => {
        sink.onClose(cleanup);
        sink.done();
        sink.close();
        sink.done();
      },
    });
    await readAll(res);
    expect(cleanup).toHaveBeenCalledTimes(1);
  });

  it('fires registered-after-close handlers immediately', async () => {
    const { req } = makeRequest();
    const cleanup = vi.fn();
    const res = sseResponse({
      signal: req.signal,
      heartbeatMs: 0,
      initialHeartbeat: false,
      setup: (sink) => {
        sink.done();
        sink.onClose(cleanup); // registered after close
      },
    });
    await readAll(res);
    expect(cleanup).toHaveBeenCalledTimes(1);
  });
});

describe('sseResponse — setup error handling', () => {
  it('emits an `error` event then closes when setup rejects', async () => {
    const { req } = makeRequest();
    const res = sseResponse({
      signal: req.signal,
      heartbeatMs: 0,
      initialHeartbeat: false,
      setup: () => Promise.reject(new Error('boom')),
    });
    const body = await readAll(res);
    expect(body).toContain('event: error');
    expect(body).toContain('"phase":"setup"');
    expect(body).toContain('"message":"boom"');
  });
});

describe('sseResponse — heartbeat timer', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('emits a heartbeat every heartbeatMs', async () => {
    const { req } = makeRequest();
    const chunks: string[] = [];
    const res = sseResponse({
      signal: req.signal,
      heartbeatMs: 500,
      initialHeartbeat: false,
      setup: () => {/* never resolves; we just want to observe heartbeats */},
    });
    const reader = res.body!.getReader();
    const dec = new TextDecoder();

    // Read available bytes (initial setup phase + first heartbeat tick).
    const readChunk = async () => {
      const { value } = await reader.read();
      if (value) chunks.push(dec.decode(value));
    };

    await vi.advanceTimersByTimeAsync(500);
    await readChunk();
    await vi.advanceTimersByTimeAsync(500);
    await readChunk();

    const joined = chunks.join('');
    // Two heartbeat frames expected.
    const matches = joined.match(/event: heartbeat/g) ?? [];
    expect(matches.length).toBeGreaterThanOrEqual(2);

    try { reader.releaseLock(); } catch { /* ignore */ }
  });
});

describe('sseResponse — sink.eventRaw', () => {
  it('emits the data string verbatim (no JSON.stringify)', async () => {
    const { req } = makeRequest();
    const res = sseResponse({
      signal: req.signal,
      heartbeatMs: 0,
      initialHeartbeat: false,
      setup: (sink) => {
        sink.eventRaw('chunk', 'hello world');
        sink.done();
      },
    });
    const body = await readAll(res);
    // Raw string passes through — not JSON-quoted.
    expect(body).toContain('event: chunk\ndata: hello world\n');
    // Sanity: event() would have wrapped it in quotes.
    expect(body).not.toContain('"hello world"');
  });

  it('still allocates a monotonic id by default', async () => {
    const { req } = makeRequest();
    const res = sseResponse({
      signal: req.signal,
      heartbeatMs: 0,
      initialHeartbeat: false,
      setup: (sink) => {
        sink.eventRaw('chunk', 'a');
        sink.eventRaw('chunk', 'b');
        sink.done();
      },
    });
    const body = await readAll(res);
    expect(body).toMatch(/id: 1\nevent: chunk\ndata: a/);
    expect(body).toMatch(/id: 2\nevent: chunk\ndata: b/);
  });

  it('respects explicit id override', async () => {
    const { req } = makeRequest();
    const res = sseResponse({
      signal: req.signal,
      heartbeatMs: 0,
      initialHeartbeat: false,
      setup: (sink) => {
        sink.eventRaw('chunk', 'a', { id: 100 });
        sink.done();
      },
    });
    const body = await readAll(res);
    expect(body).toContain('id: 100\nevent: chunk\ndata: a');
  });
});

describe('sseResponse — pre-aborted signal', () => {
  it('returns a Response that closes immediately if signal is already aborted', async () => {
    const controller = new AbortController();
    controller.abort();
    const res = sseResponse({
      signal: controller.signal,
      heartbeatMs: 0,
      initialHeartbeat: false,
      setup: () => {/* may or may not be called depending on micro-task timing */},
    });
    // Reading should resolve quickly (stream closes).
    const body = await readAll(res);
    expect(typeof body).toBe('string');
  });
});
