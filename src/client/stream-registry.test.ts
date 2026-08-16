/**
 * Live SSE stream registry — the per-host connection-budget observability that
 * made the 2026-07-26/27 "gym runs never show up" starvation visible.
 *
 * @vitest-environment jsdom
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  STREAM_BUDGET_WARN_AT,
  _resetStreamRegistry,
  countLiveStreamsForHost,
  listLiveStreams,
  registerLiveStream,
} from './stream-registry';

beforeEach(() => _resetStreamRegistry());
afterEach(() => vi.restoreAllMocks());

describe('stream registry', () => {
  it('tracks live streams and releases them on unregister', () => {
    const a = registerLiveStream('http://h1/api/zero-harness/sse');
    const b = registerLiveStream('http://h1/api/flags/stream');
    expect(listLiveStreams().map((r) => r.url)).toEqual([
      'http://h1/api/zero-harness/sse',
      'http://h1/api/flags/stream',
    ]);
    expect(countLiveStreamsForHost('h1')).toBe(2);
    a();
    expect(countLiveStreamsForHost('h1')).toBe(1);
    b();
    expect(listLiveStreams()).toEqual([]);
  });

  it('counts per host, so one host hitting the cap does not implicate another', () => {
    registerLiveStream('http://h1/a/sse');
    registerLiveStream('http://h2/b/sse');
    expect(countLiveStreamsForHost('h1')).toBe(1);
    expect(countLiveStreamsForHost('h2')).toBe(1);
  });

  it('warns once when a host reaches the budget line, naming the live stream urls', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    for (let i = 0; i < STREAM_BUDGET_WARN_AT - 1; i++) registerLiveStream(`http://h1/s${i}/sse`);
    expect(warn).not.toHaveBeenCalled(); // still under the line
    registerLiveStream('http://h1/tipping/sse');
    expect(warn).toHaveBeenCalledTimes(1);
    const msg = String(warn.mock.calls[0]?.[0] ?? '');
    expect(msg).toContain('standing SSE streams open to h1');
    expect(msg).toContain('/tipping/sse'); // enumerates what is holding slots
    registerLiveStream('http://h1/another/sse');
    expect(warn).toHaveBeenCalledTimes(1); // throttled — one warning per host
  });

  it('re-arms the warning after the host drops back under the line (later regression still warns)', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const releases = [];
    for (let i = 0; i < STREAM_BUDGET_WARN_AT; i++) releases.push(registerLiveStream(`http://h1/s${i}/sse`));
    expect(warn).toHaveBeenCalledTimes(1);
    releases[0]!(); // back under the line
    registerLiveStream('http://h1/regressed/sse'); // hits the line again
    expect(warn).toHaveBeenCalledTimes(2);
  });

  it('exposes the snapshot on window for live inspection from a running page', () => {
    registerLiveStream('http://h1/api/zero-harness/sse');
    const fn = (window as unknown as { __papercuspStreams?: () => unknown[] }).__papercuspStreams;
    expect(typeof fn).toBe('function');
    expect(fn!()).toHaveLength(1);
  });

  it('does not throw on an unparsable url (records it under a sentinel host)', () => {
    expect(() => registerLiveStream('::::not a url::::')).not.toThrow();
    expect(listLiveStreams()).toHaveLength(1);
  });
});
