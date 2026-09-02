/**
 * Cross-realm connection-budget accounting.
 *
 * WHY THIS FILE EXISTS (WI-2141694). The browser's connection limit is per
 * ORIGIN-per-process, but `stream-registry`'s `live` Map is module-scoped, so
 * its denominator is the JS REALM. A page with three same-origin iframes each
 * holding 2-3 standing streams sits at 6-9 sockets against a cap of 6 while
 * every realm reads 2-3 and stays under `STREAM_BUDGET_WARN_AT`. The registry
 * whose docstring says it exists to make exceeding the budget "observable
 * instead of mysterious" was structurally unable to see the budget.
 *
 * Measured on the portal (:3081 embedding :3070 panes): an in-pane same-origin
 * fetch was unresolved at 25s while curl answered the identical URL in ~2ms,
 * 3/3, with four browser processes each pinned at exactly 6 established
 * connections. Those measurements are the reason for this file; the tests
 * themselves need no browser, which is what makes the defect regression-proof.
 *
 * @vitest-environment jsdom
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { BudgetChannel } from './stream-registry';

const HOST = '127.0.0.1:3070';
const url = (p: string) => `http://${HOST}/api/${p}`;

/**
 * Stands in for BroadcastChannel, which jsdom does not implement.
 *
 * Delivery is SYNCHRONOUS so assertions need no scheduling; the real API is
 * async, which means in a browser the warning lands a tick after the
 * registration rather than during it. The protocol tolerates that (the count
 * is re-evaluated whenever a peer announcement arrives) — but no test here
 * should be read as a claim about timing.
 */
class FakeBus {
  private readonly handlers = new Map<object, (m: unknown) => void>();
  private members: BudgetChannel[] = [];

  connect(): BudgetChannel {
    const ch: BudgetChannel = {
      post: (message) => {
        // BroadcastChannel never delivers to its own sender.
        for (const m of this.members) {
          if (m === ch) continue;
          this.handlers.get(m)?.(JSON.parse(JSON.stringify(message)));
        }
      },
      onMessage: (h) => {
        this.handlers.set(ch, h);
      },
      close: () => {
        this.members = this.members.filter((m) => m !== ch);
        this.handlers.delete(ch);
      },
    };
    this.members.push(ch);
    return ch;
  }
}

/**
 * Load an independent copy of the registry. `vi.resetModules()` clears the
 * module cache, so each import evaluates the module body afresh and gets its
 * own `live` Map — which is exactly what a separate iframe realm gets.
 */
async function loadRealm(bus?: FakeBus) {
  vi.resetModules();
  const mod = await import('./stream-registry');
  mod._setBudgetChannelFactory(bus ? () => bus.connect() : () => null);
  return mod;
}

beforeEach(() => {
  vi.restoreAllMocks();
});

describe('module realms are genuinely independent (harness precondition)', () => {
  it('two loads of the registry do not share the live Map', async () => {
    const a = await loadRealm();
    const b = await loadRealm();

    for (const p of ['a1', 'a2', 'a3']) a.registerLiveStream(url(p));
    for (const p of ['b1', 'b2', 'b3']) b.registerLiveStream(url(p));

    // Six sockets to one host whose browser cap is six. If this ever fails,
    // the two imports are sharing state and every test below is measuring one
    // realm twice — the assertion exists to catch that, not to bless it.
    expect(a.countLiveStreamsForHost(HOST)).toBe(3);
    expect(b.countLiveStreamsForHost(HOST)).toBe(3);
  });
});

describe('origin-wide accounting', () => {
  /**
   * POSITIVE CONTROL for the whole file. The same code path is exercised twice
   * — transport absent, then transport present — and must give DIFFERENT
   * answers. Without this pairing, a cross-realm total that silently equalled
   * the local count would look exactly like a passing test.
   */
  it('reports only local streams with no transport, and the true total with one', async () => {
    // Six streams across two realms legitimately trips the budget warning;
    // this test is about the COUNT, so silence the expected console output.
    vi.spyOn(console, 'warn').mockImplementation(() => {});
    const isolated = await loadRealm(); // factory yields null — today's behaviour
    for (const p of ['s1', 's2', 's3']) isolated.registerLiveStream(url(p));
    expect(isolated.countStreamsForHostAllRealms(HOST)).toBe(3); // blind, by construction

    const bus = new FakeBus();
    const a = await loadRealm(bus);
    const b = await loadRealm(bus);
    for (const p of ['a1', 'a2', 'a3']) a.registerLiveStream(url(p));
    for (const p of ['b1', 'b2', 'b3']) b.registerLiveStream(url(p));

    // The number the browser's cap actually applies to, seen from both sides.
    expect(a.countStreamsForHostAllRealms(HOST)).toBe(6);
    expect(b.countStreamsForHostAllRealms(HOST)).toBe(6);
    // ...while each realm's own view is unchanged, so callers asking what THEY
    // can release still get their own set.
    expect(a.countLiveStreamsForHost(HOST)).toBe(3);
    expect(b.countLiveStreamsForHost(HOST)).toBe(3);
  });

  it('keeps hosts separate across realms', async () => {
    const bus = new FakeBus();
    const a = await loadRealm(bus);
    const b = await loadRealm(bus);
    a.registerLiveStream('http://h1/a/sse');
    b.registerLiveStream('http://h2/b/sse');
    expect(a.countStreamsForHostAllRealms('h1')).toBe(1);
    expect(a.countStreamsForHostAllRealms('h2')).toBe(1);
    expect(a.countStreamsForHostAllRealms('h3')).toBe(0);
  });

  it('splits the snapshot into local vs peer so a caller can see where slots went', async () => {
    const bus = new FakeBus();
    const a = await loadRealm(bus);
    const b = await loadRealm(bus);
    a.registerLiveStream(url('a1'));
    for (const p of ['b1', 'b2']) b.registerLiveStream(url(p));

    const snap = a.getStreamBudgetSnapshot(HOST);
    expect(snap).toMatchObject({ host: HOST, local: 1, peer: 2, total: 3, peerRealms: 1 });
  });
});

describe('the warning now fires on the origin-wide total', () => {
  /**
   * THE DEFECT, as a regression test. Two documents holding two streams each
   * exhaust two thirds of the budget, but neither reaches WARN_AT=4 alone, so
   * before this fix the console stayed silent all the way to the hard cap.
   */
  it('warns when peers push the origin over the line, though no realm reaches it alone', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const bus = new FakeBus();
    const a = await loadRealm(bus);
    const b = await loadRealm(bus);

    for (const p of ['a1', 'a2']) a.registerLiveStream(url(p));
    expect(warn).not.toHaveBeenCalled(); // 2 of 4 — correctly quiet

    b.registerLiveStream(url('b1'));
    expect(warn).not.toHaveBeenCalled(); // 3 of 4 — still correctly quiet

    b.registerLiveStream(url('b2')); // origin-wide total reaches 4
    expect(warn).toHaveBeenCalled();

    const msg = String(warn.mock.calls[0]?.[0] ?? '');
    expect(msg).toContain(`4 standing SSE streams open to ${HOST}`);
    // The fact the per-realm count hid: this is a cross-document problem.
    expect(msg).toContain('across 2 documents of this origin');
    expect(msg).toContain('(2 in this one)');
  });

  it('stays silent at the same stream count when they are NOT sharing an origin pool', async () => {
    // Same four streams, same two realms, no transport between them: nobody is
    // over budget as far as any realm can tell. Guards the test above against
    // passing for the trivial reason that four registrations always warn.
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const a = await loadRealm();
    const b = await loadRealm();
    for (const p of ['a1', 'a2']) a.registerLiveStream(url(p));
    for (const p of ['b1', 'b2']) b.registerLiveStream(url(p));
    expect(warn).not.toHaveBeenCalled();
  });
});

describe('peer lifecycle', () => {
  it('drops a departed realm’s streams from the total', async () => {
    const bus = new FakeBus();
    const a = await loadRealm(bus);
    const b = await loadRealm(bus);
    a.registerLiveStream(url('a1'));
    const releaseB = b.registerLiveStream(url('b1'));
    expect(a.countStreamsForHostAllRealms(HOST)).toBe(2);

    releaseB(); // b closes its stream and announces the new count
    expect(a.countStreamsForHostAllRealms(HOST)).toBe(1);
  });

  it('ages out a realm that vanished without announcing (crash, not a clean unload)', async () => {
    const bus = new FakeBus();
    const a = await loadRealm(bus);
    const b = await loadRealm(bus);
    a.registerLiveStream(url('a1'));
    b.registerLiveStream(url('b1'));
    expect(a.countStreamsForHostAllRealms(HOST)).toBe(2);

    // b is gone; it will never answer another query. Advance past the trust
    // window — the count must fall back to what a can actually see rather than
    // counting a dead document's sockets forever.
    const realNow = Date.now;
    vi.spyOn(Date, 'now').mockImplementation(() => realNow() + a.PEER_STALE_AFTER_MS + 1);
    expect(a.countStreamsForHostAllRealms(HOST)).toBe(1);
  });
});
