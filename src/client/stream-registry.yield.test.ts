/**
 * Yield on contention (WI-2141694).
 *
 * The sibling file proves the registry can SEE origin-wide pressure. This one
 * proves it ACTS on it — and, just as importantly, that it does not act on
 * streams nobody offered.
 *
 * Two measurements shape what is asserted here, and both are worth restating
 * because they rule out designs that look reasonable:
 *   - at the per-origin cap the next request does NOT error, it queues
 *     silently and forever, so the trigger must be PROACTIVE (evaluated at
 *     registration off the budget snapshot) rather than reactive;
 *   - the pool is shared across same-origin realms, so the stream that ought
 *     to yield frequently lives in a DIFFERENT document from the one starving.
 *
 * @vitest-environment jsdom
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { BudgetChannel } from './stream-registry';

const HOST = '127.0.0.1:3070';
const url = (p: string) => `http://${HOST}/api/${p}`;

/** Synchronous stand-in for BroadcastChannel (absent in jsdom). */
class FakeBus {
  private readonly handlers = new Map<object, (m: unknown) => void>();
  private members: BudgetChannel[] = [];

  connect(): BudgetChannel {
    const ch: BudgetChannel = {
      post: (message) => {
        for (const m of this.members) {
          if (m === ch) continue;
          this.handlers.get(m)?.(JSON.parse(JSON.stringify(message)));
        }
      },
      onMessage: (h) => { this.handlers.set(ch, h); },
      close: () => {
        this.members = this.members.filter((m) => m !== ch);
        this.handlers.delete(ch);
      },
    };
    this.members.push(ch);
    return ch;
  }
}

async function loadRealm(bus?: FakeBus) {
  vi.resetModules();
  const mod = await import('./stream-registry');
  mod._setBudgetChannelFactory(bus ? () => bus.connect() : () => null);
  return mod;
}

beforeEach(() => {
  vi.restoreAllMocks();
  // Every test here deliberately crosses STREAM_BUDGET_WARN_AT on its way to
  // the yield line, so the budget warning is EXPECTED output rather than a
  // surprise. `vitest-fail-on-console` fails a test that logs one, and the
  // sibling accounting suite already asserts the warning's own behaviour.
  vi.spyOn(console, 'warn').mockImplementation(() => {});
});

describe('opt-in is the whole safety story', () => {
  it('NEVER yields a stream that supplied no onYieldRequested', async () => {
    const r = await loadRealm();
    // Well past the yield line — the only thing stopping a yield is that
    // nobody volunteered. This is the default every existing caller gets.
    for (const p of ['a', 'b', 'c', 'd', 'e', 'f', 'g']) r.registerLiveStream(url(p));
    expect(r.countLiveStreamsForHost(HOST)).toBe(7);
    // Nothing to assert a callback on — the point is that reaching the cap is
    // not, by itself, permission to close somebody's stream.
  });

  it('yields an opted-in stream once the origin reaches the yield line', async () => {
    const r = await loadRealm();
    const onYieldRequested = vi.fn();

    r.registerLiveStream(url('volunteer'), { onYieldRequested });
    // Below the line: still nothing.
    for (let i = 0; i < r.STREAM_YIELD_AT - 2; i++) r.registerLiveStream(url(`filler${i}`));
    expect(onYieldRequested).not.toHaveBeenCalled();

    // The registration that crosses the line is the one that acts.
    r.registerLiveStream(url('crosses'));
    expect(onYieldRequested).toHaveBeenCalledTimes(1);
  });

  it('does not yield below the line (negative control for the trigger)', async () => {
    const r = await loadRealm();
    const onYieldRequested = vi.fn();
    r.registerLiveStream(url('volunteer'), { onYieldRequested });
    for (let i = 0; i < r.STREAM_YIELD_AT - 2; i++) r.registerLiveStream(url(`f${i}`));
    expect(r.countStreamsForHostAllRealms(HOST)).toBe(r.STREAM_YIELD_AT - 1);
    expect(onYieldRequested).not.toHaveBeenCalled();
  });
});

describe('which stream is chosen', () => {
  it('drops the LOWEST priority, not the oldest', async () => {
    const r = await loadRealm();
    const oldestButImportant = vi.fn();
    const newerButExpendable = vi.fn();

    // Registered FIRST, so "yield the oldest" would pick this one.
    r.registerLiveStream(url('chat'), { priority: 10, onYieldRequested: oldestButImportant });
    r.registerLiveStream(url('poller'), { priority: 1, onYieldRequested: newerButExpendable });
    for (let i = 0; i < r.STREAM_YIELD_AT - 2; i++) r.registerLiveStream(url(`f${i}`));

    expect(newerButExpendable).toHaveBeenCalledTimes(1);
    expect(oldestButImportant).not.toHaveBeenCalled();
  });

  it('falls back to oldest-first among equal priorities', async () => {
    const r = await loadRealm();
    const older = vi.fn();
    const newer = vi.fn();
    r.registerLiveStream(url('older'), { onYieldRequested: older });
    r.registerLiveStream(url('newer'), { onYieldRequested: newer });
    for (let i = 0; i < r.STREAM_YIELD_AT - 2; i++) r.registerLiveStream(url(`f${i}`));

    expect(older).toHaveBeenCalledTimes(1);
    expect(newer).not.toHaveBeenCalled();
  });

  it('yields at most once per host inside the cooldown', async () => {
    const r = await loadRealm();
    const first = vi.fn();
    const second = vi.fn();
    r.registerLiveStream(url('v1'), { onYieldRequested: first });
    r.registerLiveStream(url('v2'), { onYieldRequested: second });
    for (let i = 0; i < r.STREAM_YIELD_AT - 2; i++) r.registerLiveStream(url(`f${i}`));
    expect(first).toHaveBeenCalledTimes(1);

    // More pressure immediately after: the cooldown must stop a cascade that
    // would otherwise close every volunteer in one burst.
    r.registerLiveStream(url('more1'));
    r.registerLiveStream(url('more2'));
    expect(second).not.toHaveBeenCalled();
  });
});

describe('coming back', () => {
  it('invites a parked consumer back once the origin clears', async () => {
    const r = await loadRealm();
    const onResumeAllowed = vi.fn();
    let release: (() => void) | null = null;
    const onYieldRequested = vi.fn(() => { release?.(); });

    release = r.registerLiveStream(url('volunteer'), { onYieldRequested, onResumeAllowed });
    const fillers: Array<() => void> = [];
    for (let i = 0; i < r.STREAM_YIELD_AT - 1; i++) fillers.push(r.registerLiveStream(url(`f${i}`)));

    expect(onYieldRequested).toHaveBeenCalledTimes(1);
    expect(onResumeAllowed).not.toHaveBeenCalled();

    // Drain until the origin is comfortably clear. The gap between the yield
    // line and the resume line is the hysteresis: resuming at the yield line
    // would re-contend on the next registration.
    while (fillers.length && r.countStreamsForHostAllRealms(HOST) >= r.STREAM_RESUME_UNDER) {
      fillers.pop()!();
    }
    expect(onResumeAllowed).toHaveBeenCalledTimes(1);
  });

  it('invites a parked consumer EXACTLY once, however many slots free up', async () => {
    // The cooldown makes two same-host yields impossible inside one test, so
    // this does NOT assert round-robin across several parked consumers — it
    // asserts the property that is actually reachable here and that a naive
    // implementation gets wrong: draining N slots must not call one parked
    // consumer's onResumeAllowed N times.
    const r = await loadRealm();
    const onResumeAllowed = vi.fn();
    let release: (() => void) | null = null;
    release = r.registerLiveStream(url('v1'), {
      onYieldRequested: () => release?.(),
      onResumeAllowed,
    });
    const fillers: Array<() => void> = [];
    for (let i = 0; i < r.STREAM_YIELD_AT - 1; i++) fillers.push(r.registerLiveStream(url(`f${i}`)));
    expect(onResumeAllowed).not.toHaveBeenCalled();

    // Free EVERY remaining slot — several unregisters land below the resume
    // line, and each one runs maybeInviteResume.
    while (fillers.length) fillers.pop()!();
    expect(r.countStreamsForHostAllRealms(HOST)).toBe(0);
    expect(onResumeAllowed).toHaveBeenCalledTimes(1);
  });
});

describe('cross-realm arbitration', () => {
  /**
   * POSITIVE/NEGATIVE CONTROL PAIR. The same scenario runs with the transport
   * absent and present and MUST give different answers. Without the pairing, a
   * cross-realm yield that silently never fired would look identical to a
   * passing test.
   */
  it('does NOT yield across realms when there is no transport (negative control)', async () => {
    const onYieldRequested = vi.fn();
    const b = await loadRealm(); // no bus
    b.registerLiveStream(url('volunteer'), { onYieldRequested });

    const a = await loadRealm(); // no bus
    for (let i = 0; i < 6; i++) a.registerLiveStream(url(`a${i}`));

    // A is well past the cap but cannot see B, and B cannot hear A.
    expect(onYieldRequested).not.toHaveBeenCalled();
  });

  it('yields a stream in ANOTHER realm when this one is starving', async () => {
    const bus = new FakeBus();
    const onYieldRequested = vi.fn();

    const b = await loadRealm(bus);
    b.registerLiveStream(url('volunteer'), { onYieldRequested });

    const a = await loadRealm(bus);
    // A holds NOTHING yieldable; the stream that must go lives in B. This is
    // the measured shape — a realm starved by sibling iframes.
    for (let i = 0; i < a.STREAM_YIELD_AT - 1; i++) a.registerLiveStream(url(`a${i}`));

    expect(a.countStreamsForHostAllRealms(HOST)).toBe(a.STREAM_YIELD_AT);
    expect(onYieldRequested).toHaveBeenCalledTimes(1);
  });

  it('only ONE realm yields, and it is the one holding the lowest priority', async () => {
    const bus = new FakeBus();
    const importantInA = vi.fn();
    const expendableInB = vi.fn();

    const a = await loadRealm(bus);
    a.registerLiveStream(url('chat'), { priority: 10, onYieldRequested: importantInA });

    const b = await loadRealm(bus);
    b.registerLiveStream(url('poller'), { priority: 1, onYieldRequested: expendableInB });

    // Push the origin over the line from A.
    for (let i = 0; i < a.STREAM_YIELD_AT - 2; i++) a.registerLiveStream(url(`f${i}`));

    // Both realms evaluate the same total order; exactly one finds itself the
    // winner. Over-yield here would close a pane the user is watching.
    expect(expendableInB).toHaveBeenCalledTimes(1);
    expect(importantInA).not.toHaveBeenCalled();
  });
});
