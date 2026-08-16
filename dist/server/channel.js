"use strict";
/**
 * In-process broadcast channel with ring-buffer replay.
 *
 * Replaces the 4 ad-hoc EventEmitter buses that used to live in
 * apps/operator/lib/ (branch-action-bus, harness-log-bus,
 * provision-audit-bus, run-chunk-bus). Same shape, type-parameterized,
 * pinned on globalThis so HMR doesn't drop live subscribers.
 *
 * Ordering: events delivered to each subscriber in publish order.
 * Backpressure: publish never blocks. Each subscriber has its own
 * bounded queue (default 4096); on overflow, oldest is dropped and a
 * `lossy` flag is set on the next yield. Subscribers should reconnect
 * with Last-Event-ID and use `recentSince()` to recover.
 *
 * Channel key convention: prefix with your domain (e.g. `backup:`,
 * `audit:`, `jobs:`). See README.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.getChannel = getChannel;
exports.dropChannel = dropChannel;
exports.listChannels = listChannels;
exports._resetChannelsForTest = _resetChannelsForTest;
const ids_1 = require("../wire/ids");
// Brand-neutral, package-internal key so the singleton survives HMR
// without colliding with other libs on globalThis. No consuming-app or
// project coupling — any project can borrow this as-is.
const GLOBAL_KEY = '__sse_channel_registry__';
function registry() {
    const g = globalThis;
    if (!g[GLOBAL_KEY])
        g[GLOBAL_KEY] = new Map();
    return g[GLOBAL_KEY];
}
const DEFAULTS = {
    ringSize: 256,
    subscriberQueueSize: 4096,
    gcDelayMs: 60_000,
    idleReapMs: 600_000,
};
function makeState(key, opts) {
    return {
        key,
        opts: { ...DEFAULTS, ...opts },
        ring: [],
        ids: (0, ids_1.createIdAllocator)(0),
        subs: new Set(),
        syncHandlers: new Set(),
        doneHandlers: new Set(),
        isDone: false,
        donePayload: undefined,
        gcTimer: null,
        lastActivityMs: Date.now(),
    };
}
function deliver(state, item) {
    // Synchronous handlers fire first, in publish order, before any async
    // subscribers are notified. Handler throws are swallowed so a single bad
    // listener can't break the producer or starve other subscribers.
    for (const handler of state.syncHandlers) {
        try {
            handler(item);
        }
        catch { /* never crash producer */ }
    }
    for (const sub of state.subs) {
        if (sub.done)
            continue;
        if (sub.waiting) {
            const cb = sub.waiting;
            sub.waiting = null;
            cb({ value: item, done: false });
            continue;
        }
        if (sub.queue.length >= state.opts.subscriberQueueSize) {
            sub.queue.shift(); // drop oldest
        }
        sub.queue.push(item);
    }
}
function bindChannel(state) {
    return {
        publish(event) {
            if (state.isDone) {
                throw new Error(`channel ${state.key}: publish after done()`);
            }
            state.lastActivityMs = Date.now();
            const id = state.ids.next();
            const item = { id, event };
            // Ring buffer write
            state.ring.push(item);
            if (state.ring.length > state.opts.ringSize)
                state.ring.shift();
            deliver(state, item);
            return id;
        },
        done(payload) {
            if (state.isDone)
                return;
            state.isDone = true;
            state.donePayload = payload;
            // Notify all subscribers — iteration ends.
            for (const sub of state.subs) {
                sub.done = true;
                if (sub.waiting) {
                    const cb = sub.waiting;
                    sub.waiting = null;
                    cb({ value: undefined, done: true });
                }
            }
            // Fire sync done handlers AFTER async-subscriber teardown so handlers
            // observing isDone see a quiesced channel.
            for (const handler of state.doneHandlers) {
                try {
                    handler();
                }
                catch { /* never crash producer */ }
            }
            scheduleGC(state);
        },
        onPublish(handler) {
            state.syncHandlers.add(handler);
            return () => { state.syncHandlers.delete(handler); };
        },
        onDone(handler) {
            if (state.isDone) {
                try {
                    handler();
                }
                catch { /* swallow */ }
                return () => { };
            }
            state.doneHandlers.add(handler);
            return () => { state.doneHandlers.delete(handler); };
        },
        get isDone() { return state.isDone; },
        get donePayload() { return state.donePayload; },
        get recent() { return state.ring; },
        recentSince(sinceId) {
            const since = sinceId ?? 0;
            return state.ring.filter((e) => e.id > since);
        },
        subscribe() {
            if (state.gcTimer) {
                clearTimeout(state.gcTimer);
                state.gcTimer = null;
            }
            const sub = { queue: [], waiting: null, done: false };
            state.subs.add(sub);
            // If channel is already done, the subscriber sees an immediately-ended iteration
            if (state.isDone)
                sub.done = true;
            return {
                [Symbol.asyncIterator]() {
                    return {
                        next() {
                            if (sub.queue.length > 0) {
                                return Promise.resolve({ value: sub.queue.shift(), done: false });
                            }
                            if (sub.done) {
                                state.subs.delete(sub);
                                scheduleGC(state);
                                return Promise.resolve({ value: undefined, done: true });
                            }
                            return new Promise((resolve) => {
                                sub.waiting = resolve;
                            });
                        },
                        return() {
                            // Caller bailed (break, throw). Unhook the subscriber.
                            sub.done = true;
                            if (sub.waiting) {
                                const cb = sub.waiting;
                                sub.waiting = null;
                                cb({ value: undefined, done: true });
                            }
                            state.subs.delete(sub);
                            scheduleGC(state);
                            return Promise.resolve({ value: undefined, done: true });
                        },
                    };
                },
            };
        },
        get subscriberCount() { return state.subs.size; },
        get syncHandlerCount() { return state.syncHandlers.size; },
    };
}
// ── idle-reap backstop ────────────────────────────────────────────────────────
// scheduleGC only reaps a channel that called done() and has zero subscribers.
// A producer that dies WITHOUT done() (e.g. a crashed orchestrator subprocess
// that never POSTs its terminal chunk) leaves a channel that is never isDone, so
// scheduleGC's `!state.isDone` guard skips it forever — an unbounded
// per-producer leak (EI-127, cluster #5). This process-wide sweep is the
// backstop: drop any channel with zero subscribers idle past its idleReapMs,
// regardless of isDone. Cheap (one O(N) pass/min, N = live channels), unref'd so
// it never holds the process open.
const REAPER_SWEEP_MS = 60_000;
let reaperTimer = null;
function ensureReaper() {
    if (reaperTimer)
        return;
    reaperTimer = setInterval(() => {
        const now = Date.now();
        const reg = registry();
        for (const [key, state] of reg) {
            if (state.subs.size === 0 && now - state.lastActivityMs > state.opts.idleReapMs) {
                if (state.gcTimer)
                    clearTimeout(state.gcTimer);
                reg.delete(key);
            }
        }
    }, REAPER_SWEEP_MS);
    if (typeof reaperTimer.unref === 'function') {
        reaperTimer.unref();
    }
}
function scheduleGC(state) {
    if (!state.isDone || state.subs.size > 0)
        return;
    if (state.gcTimer)
        clearTimeout(state.gcTimer);
    state.gcTimer = setTimeout(() => {
        if (state.isDone && state.subs.size === 0) {
            registry().delete(state.key);
        }
    }, state.opts.gcDelayMs);
    if (typeof state.gcTimer.unref === 'function')
        state.gcTimer.unref();
}
function getChannel(key, opts = {}) {
    ensureReaper();
    const reg = registry();
    let state = reg.get(key);
    if (!state) {
        state = makeState(key, opts);
        reg.set(key, state);
    }
    else {
        state.lastActivityMs = Date.now();
        if (state.gcTimer) {
            // GC was scheduled; cancel it because someone wants this channel.
            clearTimeout(state.gcTimer);
            state.gcTimer = null;
            state.isDone = false;
            state.donePayload = undefined;
        }
    }
    return bindChannel(state);
}
function dropChannel(key) {
    const reg = registry();
    const state = reg.get(key);
    if (!state)
        return false;
    if (state.gcTimer)
        clearTimeout(state.gcTimer);
    reg.delete(key);
    return true;
}
function listChannels() {
    return Array.from(registry().values()).map((s) => ({
        key: s.key,
        subscribers: s.subs.size,
        syncHandlers: s.syncHandlers.size,
        isDone: s.isDone,
        recentCount: s.ring.length,
    }));
}
/** Test-only. Clears the global registry so each test starts clean. */
function _resetChannelsForTest() {
    for (const state of registry().values()) {
        if (state.gcTimer)
            clearTimeout(state.gcTimer);
    }
    registry().clear();
    if (reaperTimer) {
        clearInterval(reaperTimer);
        reaperTimer = null;
    }
}
