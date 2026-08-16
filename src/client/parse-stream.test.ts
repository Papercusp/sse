import { describe, it, expect } from 'vitest';
import { parseSseStream, type ParsedSseEvent } from './parse-stream';

function streamFromChunks(chunks: string[]): ReadableStream<Uint8Array> {
  const enc = new TextEncoder();
  return new ReadableStream({
    start(controller) {
      for (const c of chunks) controller.enqueue(enc.encode(c));
      controller.close();
    },
  });
}

async function collect(s: ReadableStream<Uint8Array>): Promise<ParsedSseEvent[]> {
  const out: ParsedSseEvent[] = [];
  for await (const ev of parseSseStream(s)) out.push(ev);
  return out;
}

describe('parseSseStream', () => {
  it('parses a single complete frame', async () => {
    const s = streamFromChunks(['event: foo\ndata: bar\n\n']);
    expect(await collect(s)).toEqual([{ event: 'foo', data: 'bar' }]);
  });

  it('parses id field', async () => {
    const s = streamFromChunks(['id: 7\nevent: x\ndata: y\n\n']);
    expect(await collect(s)).toEqual([{ id: '7', event: 'x', data: 'y' }]);
  });

  it('uses "message" as default event name when event: is absent', async () => {
    const s = streamFromChunks(['data: hello\n\n']);
    expect(await collect(s)).toEqual([{ event: 'message', data: 'hello' }]);
  });

  it('joins multi-line data with \\n', async () => {
    const s = streamFromChunks(['event: log\ndata: line1\ndata: line2\ndata: line3\n\n']);
    expect(await collect(s)).toEqual([{ event: 'log', data: 'line1\nline2\nline3' }]);
  });

  it('ignores comments', async () => {
    const s = streamFromChunks([': keepalive\nevent: x\ndata: y\n\n']);
    expect(await collect(s)).toEqual([{ event: 'x', data: 'y' }]);
  });

  it('handles \\r\\n, \\r, and \\n line separators interchangeably', async () => {
    const s = streamFromChunks(['event: a\r\ndata: 1\r\n\r\nevent: b\rdata: 2\r\r']);
    expect(await collect(s)).toEqual([
      { event: 'a', data: '1' },
      { event: 'b', data: '2' },
    ]);
  });

  it('parses across chunk boundaries (partial field)', async () => {
    const s = streamFromChunks(['event: x\ndat', 'a: y\n\n']);
    expect(await collect(s)).toEqual([{ event: 'x', data: 'y' }]);
  });

  it('parses across chunk boundaries (newline split)', async () => {
    const s = streamFromChunks(['event: x\ndata: y\n', '\n']);
    expect(await collect(s)).toEqual([{ event: 'x', data: 'y' }]);
  });

  it('strips one leading space after the colon', async () => {
    const s = streamFromChunks(['data:hello\n\n', 'data: hello\n\n', 'data:  hello\n\n']);
    expect(await collect(s)).toEqual([
      { event: 'message', data: 'hello' },
      { event: 'message', data: 'hello' },
      { event: 'message', data: ' hello' },         // only ONE leading space stripped
    ]);
  });

  it('strips BOM at stream start', async () => {
    const enc = new TextEncoder();
    const bom = new Uint8Array([0xEF, 0xBB, 0xBF]);
    const body = new Uint8Array([...bom, ...enc.encode('event: x\ndata: y\n\n')]);
    const s = new ReadableStream<Uint8Array>({
      start(c) { c.enqueue(body); c.close(); },
    });
    expect(await collect(s)).toEqual([{ event: 'x', data: 'y' }]);
  });

  it('survives a UTF-8 codepoint split across chunks', async () => {
    // The character "🎉" is U+1F389 = 4 bytes: F0 9F 8E 89
    const full = new TextEncoder().encode('event: x\ndata: hi 🎉\n\n');
    // Split right between byte 2 and 3 of the emoji.
    const splitPoint = full.length - 5; // after "i " before emoji's middle bytes
    const a = full.slice(0, splitPoint + 2);
    const b = full.slice(splitPoint + 2);
    const s = new ReadableStream<Uint8Array>({
      start(c) { c.enqueue(a); c.enqueue(b); c.close(); },
    });
    expect(await collect(s)).toEqual([{ event: 'x', data: 'hi 🎉' }]);
  });

  it('captures retry as a number', async () => {
    const s = streamFromChunks(['retry: 2500\nevent: x\ndata: y\n\n']);
    expect(await collect(s)).toEqual([{ event: 'x', data: 'y', retry: 2500 }]);
  });

  it('ignores unknown field names', async () => {
    const s = streamFromChunks(['weird: yes\nevent: x\ndata: y\n\n']);
    expect(await collect(s)).toEqual([{ event: 'x', data: 'y' }]);
  });

  it('persists id across events until reassigned', async () => {
    const s = streamFromChunks([
      'id: 1\nevent: x\ndata: a\n\n',
      'event: x\ndata: b\n\n',              // no id field
      'id: 5\nevent: x\ndata: c\n\n',
    ]);
    expect(await collect(s)).toEqual([
      { id: '1', event: 'x', data: 'a' },
      { id: '1', event: 'x', data: 'b' },   // inherited
      { id: '5', event: 'x', data: 'c' },
    ]);
  });

  it('returns nothing for a null body', async () => {
    const out: ParsedSseEvent[] = [];
    for await (const ev of parseSseStream(null)) out.push(ev);
    expect(out).toEqual([]);
  });
});
