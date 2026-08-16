/**
 * Wire-format test for the `state-snapshot` SSE event used by the
 * bespoke-card-improvements state channel (PRs #80–#83).
 *
 * The state-snapshot event has no special handling in libs/sse —
 * it rides the generic typed-event channel. This test pins the
 * shape so refactors of the typed-event encoder can't silently
 * break the consumer at apps/operator/lib/use-state-snapshots.ts.
 *
 * Wire shape per plan §4.2 / §5.2:
 *   event: state-snapshot
 *   id: <serial>
 *   data: {"runId":"…","version":N,"snapshot":{"openCards":[...],"toolState":{...}}}
 */
import { describe, it, expect, afterEach } from 'vitest';
import { createServer, type Server } from 'node:http';
import { sseResponse, parseSseStream } from '../index';

let activeServer: Server | null = null;

afterEach(async () => {
  if (activeServer) {
    await new Promise<void>((r) => activeServer!.close(() => r()));
    activeServer = null;
  }
});

interface SnapshotPayload {
  runId: string;
  version: number;
  snapshot: {
    openCards: Array<{
      correlationId: string;
      prompt: string;
      dataSchemaJson: Record<string, unknown>;
      presentation?: unknown;
      fallbackText?: string;
      allowDecline?: boolean;
      createdAt: number;
    }>;
    toolState?: unknown;
  };
}

async function withResponse(build: (req: Request) => Response): Promise<{ url: string }> {
  const server = createServer(async (incoming, outgoing) => {
    const url = `http://${incoming.headers.host}${incoming.url ?? ''}`;
    const headers = new Headers();
    for (const [k, v] of Object.entries(incoming.headers)) {
      if (typeof v === 'string') headers.set(k, v);
    }
    const ctrl = new AbortController();
    incoming.on('close', () => ctrl.abort());
    const req = new Request(url, { headers, signal: ctrl.signal });
    const res = build(req);
    outgoing.statusCode = res.status;
    res.headers.forEach((v, k) => outgoing.setHeader(k, v));
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
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  const addr = server.address();
  if (typeof addr !== 'object' || addr === null) throw new Error('no addr');
  return { url: `http://127.0.0.1:${addr.port}` };
}

describe('state-snapshot SSE event wire-format', () => {
  it('round-trips a snapshot with openCards + toolState', async () => {
    const handle = await withResponse((req) =>
      sseResponse<{ 'state-snapshot': SnapshotPayload }>({
        signal: req.signal,
        heartbeatMs: 0,
        initialHeartbeat: false,
        setup: (sink) => {
          sink.event(
            'state-snapshot',
            {
              runId: 'r-abc',
              version: 3,
              snapshot: {
                openCards: [
                  {
                    correlationId: 'cid-1',
                    prompt: 'pick one',
                    dataSchemaJson: { type: 'object' },
                    presentation: { kind: 'radio', options: [] },
                    fallbackText: 'Pick: A or B',
                    allowDecline: true,
                    createdAt: 1234567890,
                  },
                ],
                toolState: { count: 7, phase: 'running' },
              },
            },
            { id: 42 },
          );
          sink.done({});
        },
      }),
    );

    const res = await fetch(handle.url);
    expect(res.headers.get('content-type')).toBe('text/event-stream');

    const events: Array<{ event: string; data: string; id?: string }> = [];
    for await (const ev of parseSseStream(res.body!)) {
      if (ev.event === 'state-snapshot' || ev.event === 'done') {
        events.push({ event: ev.event, data: ev.data, id: ev.id });
      }
    }

    const snap = events.find((e) => e.event === 'state-snapshot');
    expect(snap).toBeDefined();
    expect(snap!.id).toBe('42');
    const payload = JSON.parse(snap!.data) as SnapshotPayload;
    expect(payload.runId).toBe('r-abc');
    expect(payload.version).toBe(3);
    expect(payload.snapshot.openCards).toHaveLength(1);
    expect(payload.snapshot.openCards[0].correlationId).toBe('cid-1');
    expect(payload.snapshot.toolState).toEqual({ count: 7, phase: 'running' });
  });

  it('round-trips an empty snapshot (openCards: [], no toolState) — card resolved case', async () => {
    const handle = await withResponse((req) =>
      sseResponse<{ 'state-snapshot': SnapshotPayload }>({
        signal: req.signal,
        heartbeatMs: 0,
        initialHeartbeat: false,
        setup: (sink) => {
          sink.event('state-snapshot', {
            runId: 'r-empty',
            version: 9,
            snapshot: { openCards: [] },
          });
          sink.done({});
        },
      }),
    );
    const res = await fetch(handle.url);
    const events: Array<{ event: string; data: string }> = [];
    for await (const ev of parseSseStream(res.body!)) {
      if (ev.event === 'state-snapshot' || ev.event === 'done') {
        events.push({ event: ev.event, data: ev.data });
      }
    }
    const snap = events.find((e) => e.event === 'state-snapshot');
    expect(snap).toBeDefined();
    const payload = JSON.parse(snap!.data) as SnapshotPayload;
    expect(payload.snapshot.openCards).toEqual([]);
    expect(payload.snapshot.toolState).toBeUndefined();
  });

  it('multiple state-snapshot events in a single stream are received in order', async () => {
    const handle = await withResponse((req) =>
      sseResponse<{ 'state-snapshot': SnapshotPayload }>({
        signal: req.signal,
        heartbeatMs: 0,
        initialHeartbeat: false,
        setup: (sink) => {
          for (let v = 1; v <= 5; v++) {
            sink.event(
              'state-snapshot',
              {
                runId: 'r-multi',
                version: v,
                snapshot: { openCards: [], toolState: { tick: v } },
              },
              { id: v },
            );
          }
          sink.done({});
        },
      }),
    );
    const res = await fetch(handle.url);
    const snapshots: SnapshotPayload[] = [];
    for await (const ev of parseSseStream(res.body!)) {
      if (ev.event === 'state-snapshot') {
        snapshots.push(JSON.parse(ev.data) as SnapshotPayload);
      }
    }
    expect(snapshots).toHaveLength(5);
    expect(snapshots.map((s) => s.version)).toEqual([1, 2, 3, 4, 5]);
    expect((snapshots[4].snapshot.toolState as { tick: number }).tick).toBe(5);
  });

  it('Last-Event-ID semantics: ids on state-snapshot events match the request', async () => {
    // Sender-set ids let the client request a resume point via
    // Last-Event-ID. libs/sse owns the id-allocation contract; this
    // pins that state-snapshot rides that contract.
    const handle = await withResponse((req) =>
      sseResponse<{ 'state-snapshot': SnapshotPayload }>({
        signal: req.signal,
        heartbeatMs: 0,
        initialHeartbeat: false,
        setup: (sink) => {
          sink.event(
            'state-snapshot',
            { runId: 'r', version: 1, snapshot: { openCards: [] } },
            { id: 100 },
          );
          sink.event(
            'state-snapshot',
            { runId: 'r', version: 2, snapshot: { openCards: [] } },
            { id: 101 },
          );
          sink.done({});
        },
      }),
    );
    const res = await fetch(handle.url);
    const ids: string[] = [];
    for await (const ev of parseSseStream(res.body!)) {
      if (ev.event === 'state-snapshot' && ev.id) ids.push(ev.id);
    }
    expect(ids).toEqual(['100', '101']);
  });
});
