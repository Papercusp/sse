/**
 * Pattern test: locks down the wire format emitted by the four agent-chat
 * SSE routes in apps/operator (operator-scan, operator-chat, operator-
 * converse, delegate-chat). All four wrap `runAgentChat()` — an async
 * iterator yielding { type: 'delta'|'tool_call'|'result'|'error' } — and
 * map each event to sink.event() / sink.done() / sink.event('error').
 *
 * If any route deviates from this shape, client parsers
 * (apps/operator/app/_components/voice/voice-mode.ts,
 *  apps/operator/app/_components/OperatorPanel.tsx) silently break.
 *
 * @vitest-environment node
 */
import { describe, it, expect } from 'vitest';
import { sseResponse } from '../server/response';

type AgentChatEvent =
  | { type: 'delta'; text: string }
  | { type: 'tool_call'; name: string; input: unknown }
  | { type: 'result'; finalText?: string; costUsd?: number; tokensIn?: number; tokensOut?: number }
  | { type: 'error'; message: string; stderr?: string };

async function* fakeRunAgentChat(events: AgentChatEvent[]): AsyncIterable<AgentChatEvent> {
  for (const e of events) yield e;
}

async function readAll(res: Response): Promise<string> {
  const reader = res.body!.getReader();
  const dec = new TextDecoder();
  let acc = '';
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    acc += dec.decode(value, { stream: true });
  }
  return acc;
}

function runRouteShape(events: AgentChatEvent[], opts?: { sessionId?: string }): { res: Response; abort: () => void } {
  const controller = new AbortController();
  const res = sseResponse({
    signal: controller.signal,
    initialHeartbeat: false,
    heartbeatMs: 0,
    setup: async (sink) => {
      if (opts?.sessionId) {
        sink.event('session', { agentSessionId: opts.sessionId, created: true });
      }
      let assistantText = '';
      try {
        for await (const ev of fakeRunAgentChat(events)) {
          if (ev.type === 'delta') {
            assistantText += ev.text;
            sink.event('delta', { text: ev.text });
          } else if (ev.type === 'tool_call') {
            sink.event('tool_call', { name: ev.name, input: ev.input });
          } else if (ev.type === 'result') {
            sink.done({
              costUsd: ev.costUsd,
              fullText: ev.finalText ?? assistantText,
            });
          } else if (ev.type === 'error') {
            sink.event('error', { message: ev.message, stderr: ev.stderr });
          }
        }
      } catch (e) {
        sink.event('error', { message: e instanceof Error ? e.message : String(e) });
      }
    },
  });
  return { res, abort: () => controller.abort() };
}

describe('agent-chat SSE pattern (covers operator-scan/chat/converse + delegate-chat)', () => {
  it('happy path: session, deltas, tool_call, done — id-ladder is monotonic, terminates with done', async () => {
    const { res } = runRouteShape([
      { type: 'delta', text: 'hi ' },
      { type: 'tool_call', name: 'search', input: { q: 'foo' } },
      { type: 'delta', text: 'there' },
      { type: 'result', finalText: 'hi there', costUsd: 0.01 },
    ], { sessionId: 's-1' });
    const body = await readAll(res);

    // Order: session → delta → tool_call → delta → done.
    const eventLines = body.split('\n').filter((l) => l.startsWith('event: '));
    expect(eventLines).toEqual([
      'event: session',
      'event: delta',
      'event: tool_call',
      'event: delta',
      'event: done',
    ]);

    // Each user event carries a monotonic id; done event has no id (sink.done()).
    expect(body).toMatch(/id: 1\nevent: session/);
    expect(body).toMatch(/id: 2\nevent: delta\ndata: \{"text":"hi "\}/);
    expect(body).toMatch(/id: 3\nevent: tool_call\ndata: \{"name":"search","input":\{"q":"foo"\}\}/);
    expect(body).toMatch(/id: 4\nevent: delta\ndata: \{"text":"there"\}/);
    // done shape: costUsd + fullText
    expect(body).toMatch(/event: done\ndata: \{"costUsd":0\.01,"fullText":"hi there"\}/);
  });

  it('error event lands then stream stays open until client disconnects', async () => {
    const { res, abort } = runRouteShape([
      { type: 'delta', text: 'partial' },
      { type: 'error', message: 'connection refused', stderr: 'oops' },
    ]);
    // Read up to the error frame, then abort to drain.
    const reader = res.body!.getReader();
    const dec = new TextDecoder();
    let acc = '';
    while (!/event: error/.test(acc)) {
      const { done, value } = await reader.read();
      if (done) break;
      acc += dec.decode(value, { stream: true });
    }
    abort();
    // Drain remainder.
    while (true) {
      const { done } = await reader.read();
      if (done) break;
    }
    expect(acc).toContain('event: delta');
    expect(acc).toMatch(/event: error\ndata: \{"message":"connection refused","stderr":"oops"\}/);
    // No done — error was non-terminal in this path (matches all 4 routes).
    expect(acc).not.toContain('event: done');
  });

  it('done() auto-closes — late publishes after result do not leak onto wire', async () => {
    const { res } = runRouteShape([
      { type: 'delta', text: 'x' },
      { type: 'result', costUsd: 0 },
      { type: 'delta', text: 'should-be-dropped' },
    ]);
    const body = await readAll(res);
    expect(body).toContain('"text":"x"');
    expect(body).toContain('event: done');
    expect(body).not.toContain('should-be-dropped');
  });

  it('fullText falls back to assembled deltas when finalText is omitted', async () => {
    const { res } = runRouteShape([
      { type: 'delta', text: 'partial-' },
      { type: 'delta', text: 'assembled' },
      { type: 'result' }, // no finalText
    ]);
    const body = await readAll(res);
    expect(body).toMatch(/"fullText":"partial-assembled"/);
  });
});
