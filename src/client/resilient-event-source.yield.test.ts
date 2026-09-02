/**
 * What a YIELD looks like to the CALLER (WI-2141694).
 *
 * The sibling `stream-registry.yield.test.ts` proves the registry picks the
 * right victim. This file proves the thing the victim's owner actually cares
 * about: a yield must be distinguishable from a failure.
 *
 * That is not a stylistic preference, it is what makes the option adoptable.
 * The always-present streams worth yielding are exactly the ones wired to
 * connectivity reporting — `apps/operator/lib/use-state-snapshots.ts` calls
 * `reportSyncUnreachable()` from `onError` and surfaces a consolidated
 * "operator connection lost" toast. If a deliberate yield reached `onError`,
 * enabling this option would trade a hung iframe for a false disconnect toast
 * on every contended page, and the wrapper would be manufacturing the very
 * class of wrong-but-plausible signal the resume cursor exists to remove.
 *
 * The property holds because `yieldForContention` closes the socket directly
 * and never invokes `opts.onError` — and `EventSource.close()` emits no error
 * event of its own. Both halves are load-bearing, so both are asserted here
 * rather than assumed from reading the source.
 *
 * @vitest-environment jsdom
 */
import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest';

const HOST = '127.0.0.1:3070';
const url = (p: string) => `http://${HOST}/api/${p}`;

class FakeEventSource {
  static instances: FakeEventSource[] = [];
  readonly url: string;
  readonly listeners: Array<{ type: string; fn: (ev: unknown) => void }> = [];
  closed = false;
  constructor(url: string) {
    this.url = url;
    FakeEventSource.instances.push(this);
  }
  addEventListener(type: string, fn: (ev: unknown) => void) {
    this.listeners.push({ type, fn });
  }
  removeEventListener() { /* not exercised here */ }
  close() { this.closed = true; }
  fire(type: string, data: unknown = '') {
    for (const e of this.listeners) if (e.type === type) e.fn({ data, type });
  }
}

/** Fresh module graph so the wrapper and the registry share ONE instance. */
async function loadRealm() {
  vi.resetModules();
  const registry = await import('./stream-registry');
  registry._setBudgetChannelFactory(() => null); // single realm; no BroadcastChannel in jsdom
  const wrapper = await import('./resilient-event-source');
  return { registry, wrapper };
}

beforeEach(() => {
  FakeEventSource.instances = [];
  vi.restoreAllMocks();
  // Crossing the yield line necessarily crosses STREAM_BUDGET_WARN_AT first;
  // the sibling accounting suite owns that warning's behaviour.
  vi.spyOn(console, 'warn').mockImplementation(() => {});
});
afterEach(() => {
  vi.restoreAllMocks();
});

describe('a yield is not a failure', () => {
  it('does NOT call onError when the registry asks the stream to step aside', async () => {
    const { registry, wrapper } = await loadRealm();
    const onError = vi.fn();
    const onStatusChange = vi.fn();

    const source = wrapper.createResilientEventSource({
      url: url('operator/state-snapshot'),
      eventSourceCtor: FakeEventSource as unknown as typeof EventSource,
      yieldOnContention: true,
      streamPriority: -10,
      onError,
      onStatusChange,
      handlers: {},
    });
    FakeEventSource.instances[0]!.fire('open');
    expect(onError).not.toHaveBeenCalled();

    // Drive the origin past the yield line with plain, non-volunteering
    // streams, so the ONLY stream eligible to yield is ours.
    for (let i = 0; i < registry.STREAM_YIELD_AT - 1; i++) {
      registry.registerLiveStream(url(`filler${i}`));
    }

    // The yield happened...
    expect(FakeEventSource.instances[0]!.closed).toBe(true);
    expect(onStatusChange).toHaveBeenCalledWith('idle');
    // ...and it did not masquerade as a connection failure. This is the
    // assertion that makes the option safe for connectivity-reporting callers.
    expect(onError).not.toHaveBeenCalled();

    source.close();
  });

  it('reports idle, never failing, so a status-driven reporter cannot misread it', async () => {
    const { registry, wrapper } = await loadRealm();
    const onStatusChange = vi.fn();

    const source = wrapper.createResilientEventSource({
      url: url('operator/state-snapshot'),
      eventSourceCtor: FakeEventSource as unknown as typeof EventSource,
      yieldOnContention: true,
      onStatusChange,
      handlers: {},
    });
    FakeEventSource.instances[0]!.fire('open');
    for (let i = 0; i < registry.STREAM_YIELD_AT - 1; i++) {
      registry.registerLiveStream(url(`filler${i}`));
    }

    expect(onStatusChange).toHaveBeenCalledWith('idle');
    expect(onStatusChange).not.toHaveBeenCalledWith('failing');

    source.close();
  });

  it('NEGATIVE CONTROL: a real socket error still reaches onError while opted in', async () => {
    // Without this, the first test would also pass if `onError` were simply
    // never wired up on an opted-in stream — which would be a far worse bug
    // than the one being guarded, and indistinguishable from success.
    const { wrapper } = await loadRealm();
    const onError = vi.fn();

    const source = wrapper.createResilientEventSource({
      url: url('operator/state-snapshot'),
      eventSourceCtor: FakeEventSource as unknown as typeof EventSource,
      yieldOnContention: true,
      maxConsecutiveFailures: 1,
      onError,
      handlers: {},
    });
    FakeEventSource.instances[0]!.fire('open');
    FakeEventSource.instances[0]!.fire('error');

    expect(onError).toHaveBeenCalled();

    source.close();
  });
});
