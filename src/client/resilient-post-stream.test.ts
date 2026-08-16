import { describe, it, expect, vi } from 'vitest';
import { resilientPostStream, type ResilientStreamEvent } from './resilient-post-stream';

function sseStream(s: string): ReadableStream<Uint8Array> {
  const enc = new TextEncoder();
  return new ReadableStream({
    start(c) { c.enqueue(enc.encode(s)); c.close(); },
  });
}

function mockRes(body: string, headers: Record<string, string> = {}): Response {
  return {
    ok: true,
    body: sseStream(body),
    headers: { get: (k: string) => headers[k] ?? null },
  } as unknown as Response;
}

async function collect<T>(gen: AsyncGenerator<ResilientStreamEvent<T>>): Promise<ResilientStreamEvent<T>[]> {
  const out: ResilientStreamEvent<T>[] = [];
  for await (const ev of gen) out.push(ev);
  return out;
}

const noDelay = () => Promise.resolve();
type Evt = { type: string; t?: string };
const isDone = (d: unknown) => (d as Evt).type === 'done' || (d as Evt).type === 'error';

describe('resilientPostStream', () => {
  it('yields parsed events and stops at the terminal event', async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      mockRes('data: {"type":"chunk","t":"a"}\n\nid: 1\ndata: {"type":"done"}\n\n'),
    );
    const events = await collect(
      resilientPostStream<Evt>({ url: '/x', buildBody: () => ({}), isTerminal: isDone, fetchImpl, delayImpl: noDelay }),
    );
    expect(events.map((e) => e.data.type)).toEqual(['chunk', 'done']);
    expect(events.at(-1)?.terminal).toBe(true);
    expect(fetchImpl).toHaveBeenCalledTimes(1); // terminal ⇒ no resume
  });

  it('skips heartbeat events and non-JSON / empty data', async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      mockRes('event: heartbeat\ndata: {"tsMs":1}\n\ndata: notjson\n\ndata: {"type":"done"}\n\n'),
    );
    const events = await collect(
      resilientPostStream<Evt>({ url: '/x', buildBody: () => ({}), isTerminal: isDone, fetchImpl, delayImpl: noDelay }),
    );
    expect(events.map((e) => e.data.type)).toEqual(['done']);
  });

  it('resumes the same turn with Last-Event-ID when the stream drops before terminal', async () => {
    const fetchImpl = vi
      .fn()
      // 1st: a chunk (id 1) then the stream ENDS without a terminal event
      .mockResolvedValueOnce(mockRes('id: 1\ndata: {"type":"chunk","t":"a"}\n\n', { 'X-Turn-Id': 'turn-42' }))
      // 2nd (resume): the terminal event
      .mockResolvedValueOnce(mockRes('id: 2\ndata: {"type":"done"}\n\n'));
    const buildBody = vi.fn((resume: unknown) => (resume ? { resume } : { first: true }));

    const events = await collect(
      resilientPostStream<Evt>({
        url: '/x', buildBody, isTerminal: isDone, turnIdHeader: 'X-Turn-Id',
        fetchImpl, delayImpl: noDelay,
      }),
    );

    expect(events.map((e) => e.data.type)).toEqual(['chunk', 'done']);
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    // The resume request carried Last-Event-ID: 1 ...
    const resumeInit = fetchImpl.mock.calls[1][1] as RequestInit;
    expect((resumeInit.headers as Record<string, string>)['Last-Event-ID']).toBe('1');
    // ... and buildBody got the resume cursor (turnId from the header, lastEventId 1).
    expect(buildBody).toHaveBeenLastCalledWith({ turnId: 'turn-42', lastEventId: 1 });
  });

  it('gives up after maxResumeAttempts and stops cleanly', async () => {
    // Always ends without a terminal ⇒ exhausts the resume budget.
    const fetchImpl = vi.fn().mockResolvedValue(
      mockRes('id: 1\ndata: {"type":"chunk"}\n\n', { 'X-Turn-Id': 't' }),
    );
    const events = await collect(
      resilientPostStream<Evt>({
        url: '/x', buildBody: () => ({}), isTerminal: isDone, turnIdHeader: 'X-Turn-Id',
        maxResumeAttempts: 2, fetchImpl, delayImpl: noDelay,
      }),
    );
    // 1 initial + 2 resume attempts = 3 fetches, each yielding the chunk.
    expect(fetchImpl).toHaveBeenCalledTimes(3);
    expect(events.every((e) => e.data.type === 'chunk')).toBe(true);
  });
});
