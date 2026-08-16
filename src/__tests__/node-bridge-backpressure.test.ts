/**
 * node-bridge-backpressure.test.ts — P2-4
 * (operator-scalability-event-loop-2026-06-16).
 *
 * `sseResponseToNode` pumps a Web `ReadableStream` body into a Node
 * `ServerResponse`. Before P2-4 it ignored `res.write()`'s boolean, so a slow
 * client made Node buffer every SSE frame in heap (unbounded growth under many
 * slow readers). These tests pin the fix: on write()===false the pump PAUSES
 * until 'drain', resumes after, stops on client disconnect, and still works for
 * a minimal response surface that can't signal drain at all.
 */
import { describe, expect, it, vi } from 'vitest';
import { sseResponseToNode, type NodeResponseLike } from '../server/node-bridge';

function makeWebResponse(chunks: Uint8Array[]): Response {
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const c of chunks) controller.enqueue(c);
      controller.close();
    },
  });
  return new Response(body, {
    status: 200,
    headers: { 'content-type': 'text/event-stream' },
  });
}

/** Mock Node response with a controllable write() return + a tiny event bus. */
class MockRes implements NodeResponseLike {
  statusCode = 0;
  headers: Record<string, string> = {};
  written: Uint8Array[] = [];
  ended = false;
  destroyed = false;
  writableEnded = false;
  /** Per-write return: true = accepted, false = congested. */
  writeReturns: (writeIndex: number) => boolean = () => true;
  private listeners: Record<string, Array<() => void>> = {};
  private writeCount = 0;

  setHeader(name: string, value: string): void {
    this.headers[name] = value;
  }
  write(chunk: Uint8Array): boolean {
    this.written.push(chunk);
    return this.writeReturns(this.writeCount++);
  }
  end(): void {
    this.ended = true;
    this.writableEnded = true;
  }
  once(event: 'drain' | 'close' | 'error', cb: () => void): void {
    (this.listeners[event] ??= []).push(cb);
  }
  emit(event: string): void {
    const ls = this.listeners[event] ?? [];
    this.listeners[event] = [];
    for (const l of ls) l();
  }
  hasListener(event: string): boolean {
    return (this.listeners[event]?.length ?? 0) > 0;
  }
}

const CHUNKS = [new Uint8Array([1]), new Uint8Array([2]), new Uint8Array([3])];

describe('sseResponseToNode — backpressure (P2-4)', () => {
  it('copies status + headers and writes every frame on a fast client', async () => {
    const res = new MockRes();
    sseResponseToNode(makeWebResponse(CHUNKS), res);
    await vi.waitFor(() => expect(res.ended).toBe(true));
    expect(res.statusCode).toBe(200);
    expect(res.headers['content-type']).toBe('text/event-stream');
    expect(res.written.length).toBe(3);
  });

  it('PAUSES on write()===false and resumes only after drain', async () => {
    const res = new MockRes();
    // First frame congests the socket; the rest would succeed.
    res.writeReturns = (i) => i !== 0;
    sseResponseToNode(makeWebResponse(CHUNKS), res);

    // The pump must park on 'drain' after the congesting write — NOT keep
    // buffering frames 2 + 3 into the (full) socket.
    await vi.waitFor(() => expect(res.hasListener('drain')).toBe(true));
    expect(res.written.length).toBe(1);
    expect(res.ended).toBe(false);

    // Socket clears → pump resumes and finishes.
    res.emit('drain');
    await vi.waitFor(() => expect(res.ended).toBe(true));
    expect(res.written.length).toBe(3);
  });

  it('stops pumping when the client disconnects while parked on drain', async () => {
    const res = new MockRes();
    res.writeReturns = (i) => i !== 0; // congest after the first frame
    sseResponseToNode(makeWebResponse(CHUNKS), res);

    await vi.waitFor(() => expect(res.hasListener('drain')).toBe(true));
    // Client goes away mid-backpressure.
    res.destroyed = true;
    res.emit('close');

    // Pump exits without writing the remaining frames.
    await vi.waitFor(() => expect(res.ended).toBe(true));
    expect(res.written.length).toBe(1);
  });

  it('falls back to fire-and-forget when the response cannot signal drain', async () => {
    // Minimal NodeResponseLike (no once/destroyed) — a caller that predates the
    // backpressure-capable surface must still complete, not hang.
    const written: Uint8Array[] = [];
    let ended = false;
    const res: NodeResponseLike = {
      statusCode: 0,
      setHeader() {},
      write(c) {
        written.push(c);
        return false; // always "congested", but no way to await drain
      },
      end() {
        ended = true;
      },
    };
    sseResponseToNode(makeWebResponse([new Uint8Array([1]), new Uint8Array([2])]), res);
    await vi.waitFor(() => expect(ended).toBe(true));
    expect(written.length).toBe(2);
  });
});
