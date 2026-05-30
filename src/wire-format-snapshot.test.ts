/**
 * WIRE-FORMAT STABILITY CONTRACT.
 *
 * This test pins the BYTE-EXACT output of @papercusp/sse against the format
 * documented in libs/sse/README.md. Changing it requires:
 *
 *   1. A major version bump of @papercusp/sse.
 *   2. A coordinated migration of every shipped desktop client that
 *      consumes events from any production SSE route (notably the
 *      ZeroSync SSEAdapter mounted by HarnessZeroProvider when
 *      runtime === 'tauri').
 *
 * Casual edits to this snapshot WILL break desktop sync silently. If you
 * are reviewing a PR that updates the inline snapshot below, demand an
 * explanation + version-bump commit before approving.
 */

import { describe, it, expect } from 'vitest';
import { sseResponse } from './server/response';
import { encodeFrame, heartbeatFrame } from './wire/format';
import type { SyncSseEventVocabulary } from './sync-events';

async function bodyToString(res: Response): Promise<string> {
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

describe('wire-format snapshot — the bytes-on-the-wire that desktop clients depend on', () => {
  it('encodeFrame: id → event → data → retry, terminated by \\n\\n', () => {
    const bytes = encodeFrame({ id: 1, event: 'update', data: '{"x":1}' });
    expect(new TextDecoder().decode(bytes)).toMatchInlineSnapshot(`
      "id: 1
      event: update
      data: {"x":1}

      "
    `);
  });

  it('heartbeatFrame: no id, event=heartbeat, data={tsMs:<n>}', () => {
    expect(new TextDecoder().decode(heartbeatFrame(12345))).toMatchInlineSnapshot(`
      "event: heartbeat
      data: {"tsMs":12345}

      "
    `);
  });

  it('sseResponse SyncSseEventVocabulary frames are byte-exact', async () => {
    const controller = new AbortController();
    const res = sseResponse<SyncSseEventVocabulary<{ rows: number[] }>>({
      signal: controller.signal,
      heartbeatMs: 0,
      initialHeartbeat: false,
      setup: (sink) => {
        sink.event('update', { name: 'foo', args: { x: 1 }, data: { rows: [1, 2] }, tsMs: 1700_000_000_000 }, { id: 1 });
        sink.event('invalidate', { name: 'bar', args: { y: 2 }, tsMs: 1700_000_000_001 }, { id: 2 });
        sink.heartbeat();
        sink.done({ reason: 'completed' });
      },
    });
    const body = await bodyToString(res);
    // Heartbeat carries a Date.now() timestamp we can't pin without mocking;
    // normalize only the heartbeat frame's tsMs (not the explicit ones above).
    const normalized = body.replace(
      /(event: heartbeat\ndata: \{"tsMs":)\d+(\})/,
      '$1<NOW>$2',
    );
    expect(normalized).toMatchInlineSnapshot(`
      "id: 1
      event: update
      data: {"name":"foo","args":{"x":1},"data":{"rows":[1,2]},"tsMs":1700000000000}

      id: 2
      event: invalidate
      data: {"name":"bar","args":{"y":2},"tsMs":1700000000001}

      event: heartbeat
      data: {"tsMs":<NOW>}

      event: done
      data: {"reason":"completed"}

      "
    `);
  });

  it('multi-line data: one data: line per \\n in payload', () => {
    const bytes = encodeFrame({ event: 'log', data: 'line1\nline2\nline3' });
    expect(new TextDecoder().decode(bytes)).toMatchInlineSnapshot(`
      "event: log
      data: line1
      data: line2
      data: line3

      "
    `);
  });

  it('omits event line when event name is the spec default "message"', () => {
    const bytes = encodeFrame({ id: 99, event: 'message', data: 'hi' });
    expect(new TextDecoder().decode(bytes)).toMatchInlineSnapshot(`
      "id: 99
      data: hi

      "
    `);
  });

  it('uses LF only (never CRLF) — anywhere in the output', () => {
    const bytes = encodeFrame({ id: 1, event: 'multi', data: 'a\nb\nc', retry: 500 });
    const text = new TextDecoder().decode(bytes);
    expect(text.includes('\r')).toBe(false);
  });
});
