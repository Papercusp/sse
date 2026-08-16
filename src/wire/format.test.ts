import { describe, it, expect } from 'vitest';
import { encodeFrame, heartbeatFrame, commentFrame } from './format';

const dec = (b: Uint8Array) => new TextDecoder().decode(b);

describe('encodeFrame', () => {
  it('emits id → event → data → retry in that order, terminated by \\n\\n', () => {
    const bytes = encodeFrame({ id: 1, event: 'update', data: '{"x":1}', retry: 500 });
    expect(dec(bytes)).toBe('id: 1\nevent: update\ndata: {"x":1}\nretry: 500\n\n');
  });

  it('omits id line when id is undefined', () => {
    const bytes = encodeFrame({ event: 'heartbeat', data: '{"tsMs":1}' });
    expect(dec(bytes)).toBe('event: heartbeat\ndata: {"tsMs":1}\n\n');
  });

  it('omits event line when event is undefined', () => {
    const bytes = encodeFrame({ id: 5, data: 'hi' });
    expect(dec(bytes)).toBe('id: 5\ndata: hi\n\n');
  });

  it("omits event line when event is the spec default 'message'", () => {
    const bytes = encodeFrame({ event: 'message', data: 'hi' });
    expect(dec(bytes)).toBe('data: hi\n\n');
  });

  it('splits multi-line data on \\n into multiple data: lines', () => {
    const bytes = encodeFrame({ event: 'log', data: 'line1\nline2\nline3' });
    expect(dec(bytes)).toBe('event: log\ndata: line1\ndata: line2\ndata: line3\n\n');
  });

  it('emits one empty data: line for empty string payload', () => {
    const bytes = encodeFrame({ event: 'ping', data: '' });
    expect(dec(bytes)).toBe('event: ping\ndata: \n\n');
  });

  it('accepts string ids', () => {
    const bytes = encodeFrame({ id: 'abc-123', event: 'x', data: 'y' });
    expect(dec(bytes)).toBe('id: abc-123\nevent: x\ndata: y\n\n');
  });

  it('uses LF only — never CRLF', () => {
    const bytes = encodeFrame({ id: 1, event: 'x', data: 'y' });
    expect(dec(bytes).includes('\r')).toBe(false);
  });

  it('round-trips UTF-8 (emoji, CJK) without truncation', () => {
    const payload = '日本語 🎉 αβγ';
    const bytes = encodeFrame({ event: 'i18n', data: payload });
    expect(dec(bytes)).toBe(`event: i18n\ndata: ${payload}\n\n`);
  });
});

describe('heartbeatFrame', () => {
  it('builds `event: heartbeat\\ndata: {"tsMs":<now>}\\n\\n` with no id', () => {
    expect(dec(heartbeatFrame(12345))).toBe('event: heartbeat\ndata: {"tsMs":12345}\n\n');
  });

  it('uses Date.now() when no argument is passed', () => {
    const a = dec(heartbeatFrame());
    expect(a).toMatch(/^event: heartbeat\ndata: \{"tsMs":\d+\}\n\n$/);
  });
});

describe('commentFrame', () => {
  it('builds `: <text>\\n\\n`', () => {
    expect(dec(commentFrame('keepalive'))).toBe(': keepalive\n\n');
  });
});
