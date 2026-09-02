/**
 * Resume cursor round-trip (WI-2141694).
 *
 * The bug this pins: `lastEventId` was tracked by the client wrapper from the
 * day it was written and never read back out. Every deliberate close ends the
 * EventSource instance, and a NEW instance sends no `Last-Event-ID` header
 * (that header is instance state the browser owns — a caller cannot set it),
 * so a resumed stream introduced itself to the server as a brand-new
 * subscriber. Nothing errored either way: endpoints supplying `replay`
 * re-delivered their whole ring buffer, endpoints without it silently skipped
 * everything emitted while the socket was closed.
 *
 * `lei-reconnect-resume.test.ts` could not catch it — that test drives a raw
 * HTTP client and sets the header by hand, so it proves the SERVER resumes
 * correctly and never exercises this wrapper at all.
 *
 * Both halves are asserted in ONE file on purpose: the client WRITES the
 * cursor and the server READS it, and the whole point of the shared
 * `RESUME_CURSOR_PARAM` is that those two cannot drift apart. Splitting them
 * would let each side keep passing against its own spelling.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createResilientEventSource as createRaw } from './resilient-event-source';
import { parseLastEventId } from '../server/response';
import { RESUME_CURSOR_PARAM, withResumeCursor } from '../wire/resume-cursor';

class FakeEventSource {
  static instances: FakeEventSource[] = [];
  static lastUrl: string | null = null;
  readonly url: string;
  readonly listeners: Array<{ type: string; fn: (ev: any) => void }> = [];
  closed = false;
  readyState = 0;

  constructor(url: string) {
    this.url = url;
    FakeEventSource.lastUrl = url;
    FakeEventSource.instances.push(this);
  }
  addEventListener(type: string, fn: (ev: any) => void) { this.listeners.push({ type, fn }); }
  close() { this.closed = true; this.readyState = 2; }
  fire(type: string, data: any = '', lastEventId?: string) {
    for (const e of this.listeners) if (e.type === type) e.fn({ data, type, lastEventId });
  }
  fireOpen() { this.readyState = 1; this.fire('open'); }
}

const active: Array<ReturnType<typeof createRaw>> = [];
const create = (...args: Parameters<typeof createRaw>) => {
  const s = createRaw(...args);
  active.push(s);
  return s;
};
const latest = () => FakeEventSource.instances[FakeEventSource.instances.length - 1]!;
const urlsOf = () => FakeEventSource.instances.map((i) => i.url);

beforeEach(() => {
  FakeEventSource.instances = [];
  FakeEventSource.lastUrl = null;
});
afterEach(() => {
  for (const s of active.splice(0)) s.close();
});

describe('resume cursor — client writes it', () => {
  it('sends NO cursor on the first connect (there is nothing to resume from)', () => {
    create({ url: 'http://x/sse', handlers: {}, eventSourceCtor: FakeEventSource as any });
    expect(FakeEventSource.lastUrl).toBe('http://x/sse');
    expect(FakeEventSource.lastUrl).not.toContain(RESUME_CURSOR_PARAM);
  });

  it('THE BUG: carries the last seen event id on a reconnect', () => {
    const handle = create({
      url: 'http://x/sse',
      handlers: { update: () => {} },
      eventSourceCtor: FakeEventSource as any,
    });
    latest().fireOpen();
    latest().fire('update', 'payload', '42');
    expect(handle.lastEventId).toBe('42');

    handle.reconnect();

    // Before the fix this was 'http://x/sse' — a resumed stream announcing
    // itself as a brand-new subscriber.
    expect(FakeEventSource.lastUrl).toBe(`http://x/sse?${RESUME_CURSOR_PARAM}=42`);
  });

  it('does not ACCUMULATE cursors across repeated reconnects', () => {
    const handle = create({
      url: 'http://x/sse',
      handlers: { update: () => {} },
      eventSourceCtor: FakeEventSource as any,
    });
    latest().fireOpen();
    latest().fire('update', 'a', '1');
    handle.reconnect();
    latest().fireOpen();
    latest().fire('update', 'b', '2');
    handle.reconnect();

    const url = FakeEventSource.lastUrl!;
    expect(url).toBe(`http://x/sse?${RESUME_CURSOR_PARAM}=2`);
    // The regression this guards: building each URL from the previous one
    // instead of from the pristine url yields ?lastEventId=1&lastEventId=2.
    expect(url.match(new RegExp(RESUME_CURSOR_PARAM, 'g'))).toHaveLength(1);
  });

  it('joins with & when the stream URL already has a query string', () => {
    const handle = create({
      url: 'http://x/sse?topic=chat',
      handlers: { update: () => {} },
      eventSourceCtor: FakeEventSource as any,
    });
    latest().fireOpen();
    latest().fire('update', 'a', '7');
    handle.reconnect();
    expect(FakeEventSource.lastUrl).toBe(`http://x/sse?topic=chat&${RESUME_CURSOR_PARAM}=7`);
  });

  it('DROPS the cursor on setUrl — a different URL is a different stream', () => {
    const handle = create({
      url: 'http://x/sse',
      handlers: { update: () => {} },
      eventSourceCtor: FakeEventSource as any,
    });
    latest().fireOpen();
    latest().fire('update', 'a', '99');
    handle.setUrl('http://x/other');

    // Resuming a NEW subscription from a FOREIGN stream's id would ask the
    // server to skip everything below an unrelated number.
    expect(FakeEventSource.lastUrl).toBe('http://x/other');
    expect(handle.lastEventId).toBeNull();
  });

  it('every constructed URL is derived from the pristine url', () => {
    const handle = create({
      url: 'http://x/sse',
      handlers: { update: () => {} },
      eventSourceCtor: FakeEventSource as any,
    });
    latest().fireOpen();
    latest().fire('update', 'a', '5');
    handle.reconnect();
    for (const u of urlsOf()) expect(u.startsWith('http://x/sse')).toBe(true);
  });
});

describe('resume cursor — server reads it', () => {
  const req = (url: string, headers: Record<string, string> = {}) =>
    new Request(url, { headers });

  it('reads the cursor the client actually writes (the anti-drift pin)', () => {
    // Deliberately built with the client's own helper rather than a literal:
    // if the two sides ever stop agreeing, this fails instead of silently
    // degrading to "no resume".
    const url = withResumeCursor('http://x/sse', '17');
    expect(parseLastEventId(req(url))).toBe(17);
  });

  it('still reads the Last-Event-ID header', () => {
    expect(parseLastEventId(req('http://x/sse', { 'Last-Event-ID': '9' }))).toBe(9);
  });

  it('lets the HEADER win over a stale param', () => {
    // The browser's own native reconnect holds the true instance cursor; a
    // param left on the URL must never override it.
    const url = withResumeCursor('http://x/sse', '3');
    expect(parseLastEventId(req(url, { 'Last-Event-ID': '11' }))).toBe(11);
  });

  it('returns null when neither is present', () => {
    expect(parseLastEventId(req('http://x/sse'))).toBeNull();
  });

  it('rejects a malformed param exactly as it rejects a malformed header', () => {
    expect(parseLastEventId(req(`http://x/sse?${RESUME_CURSOR_PARAM}=abc`))).toBeNull();
    expect(parseLastEventId(req(`http://x/sse?${RESUME_CURSOR_PARAM}=-1`))).toBeNull();
  });
});

/**
 * FALSIFIABILITY CONTROLS.
 *
 * A guard that has never failed is a guard nobody has tested, and every
 * assertion above passes today. These pin that they can FAIL, using the
 * sanctioned pattern for a subject you `import`: keep deliberately-wrong
 * implementations permanently beside the real one, rather than mutating the
 * shared tree (which git-sync can commit mid-probe, trap or no trap).
 *
 * The calibration case is load-bearing: without it, a `predicate` that is
 * simply broken would reject the real implementation too and every control
 * would "pass" while proving nothing.
 */
describe('withResumeCursor — falsifiability controls', () => {
  /** What the client half actually has to guarantee, as one predicate. */
  const buildsCorrectResumeUrl = (impl: (u: string, c: string | null) => string): boolean => {
    const hasOneCursor = (u: string) =>
      (u.match(new RegExp(RESUME_CURSOR_PARAM, 'g')) ?? []).length === 1;
    return (
      // carries the cursor at all
      impl('http://x/sse', '1') === `http://x/sse?${RESUME_CURSOR_PARAM}=1`
      // joins correctly onto an existing query string
      && impl('http://x/sse?topic=chat', '7') === `http://x/sse?topic=chat&${RESUME_CURSOR_PARAM}=7`
      && hasOneCursor(impl('http://x/sse?topic=chat', '7'))
      // and stays out of the way when there is nothing to resume from
      && impl('http://x/sse', null) === 'http://x/sse'
    );
  };

  it('CALIBRATION: the real implementation satisfies the predicate', () => {
    expect(buildsCorrectResumeUrl((u, c) => withResumeCursor(u, c))).toBe(true);
  });

  it('CONTROL: an implementation that ignores the cursor is REJECTED', () => {
    // This is precisely the pre-fix behaviour — connect() passed the bare url.
    expect(buildsCorrectResumeUrl((u) => u)).toBe(false);
  });

  it('CONTROL: an implementation that always uses ? is REJECTED', () => {
    // Produces 'http://x/sse?topic=chat?lastEventId=7' — a malformed URL whose
    // cursor the server would never parse.
    expect(
      buildsCorrectResumeUrl((u, c) => (c == null ? u : `${u}?${RESUME_CURSOR_PARAM}=${c}`)),
    ).toBe(false);
  });

  it('CONTROL: an implementation that appends unconditionally is REJECTED', () => {
    expect(
      buildsCorrectResumeUrl((u, c) => `${u}${u.includes('?') ? '&' : '?'}${RESUME_CURSOR_PARAM}=${c}`),
    ).toBe(false);
  });

  it('CONTROL: the SERVER reader rejects a param under a different name', () => {
    // The drift this shared constant exists to prevent: client writes one
    // spelling, server reads another, and the resume silently degrades to
    // "brand-new subscriber" with nothing erroring on either side.
    expect(parseLastEventId(new Request('http://x/sse?last_event_id=17'))).toBeNull();
    expect(parseLastEventId(new Request(withResumeCursor('http://x/sse', '17')))).toBe(17);
  });
});

describe('withResumeCursor', () => {
  it('is a no-op without a cursor', () => {
    expect(withResumeCursor('http://x/sse', null)).toBe('http://x/sse');
    expect(withResumeCursor('http://x/sse', undefined)).toBe('http://x/sse');
    expect(withResumeCursor('http://x/sse', '')).toBe('http://x/sse');
  });

  it('works on a relative URL (no URL base available in a non-DOM host)', () => {
    expect(withResumeCursor('/api/sync/stream', '4')).toBe(
      `/api/sync/stream?${RESUME_CURSOR_PARAM}=4`,
    );
  });

  it('encodes a cursor that is not a bare integer', () => {
    expect(withResumeCursor('http://x/sse', 'a b&c')).toBe(
      `http://x/sse?${RESUME_CURSOR_PARAM}=a%20b%26c`,
    );
  });
});
