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
export interface BusChannel<T> {
    /** Publish an event. Assigns auto-id, notifies subscribers. Returns the id. */
    publish(event: T): number;
    /** Mark terminated. Active subscribers' iteration ends. New subscribers see done immediately. Idempotent. */
    done(payload?: unknown): void;
    readonly isDone: boolean;
    readonly donePayload: unknown | undefined;
    /** Last N events (≤ ringSize). Each carries its assigned id. */
    readonly recent: ReadonlyArray<{
        id: number;
        event: T;
    }>;
    /** Items from recent with id > sinceId. Pass null/undefined for the whole buffer. */
    recentSince(sinceId: number | null | undefined): ReadonlyArray<{
        id: number;
        event: T;
    }>;
    /**
     * Subscribe. Yields each future publish in order. Iteration ends on done().
     * Does NOT yield `recent` — use recentSince() if you want backfill.
     */
    subscribe(): AsyncIterable<{
        id: number;
        event: T;
    }>;
    /**
     * Synchronous handler subscription. Called from within publish() before it
     * returns — useful for in-process fan-out where consumers rely on
     * publish-order, same-tick delivery. Returns an unsubscribe function.
     * Handler throws are caught and swallowed.
     * For SSE bridging prefer `subscribe()` + bridgeChannel — async iteration
     * pairs naturally with stream writes.
     */
    onPublish(handler: (item: {
        id: number;
        event: T;
    }) => void): () => void;
    /**
     * Synchronous notification when the channel is marked done(). Fires
     * immediately if already done. Returns an unsubscribe function.
     */
    onDone(handler: () => void): () => void;
    readonly subscriberCount: number;
    /** Number of synchronous handlers currently registered via onPublish. */
    readonly syncHandlerCount: number;
}
export interface ChannelOptions {
    /** Ring buffer size. Default 256. */
    ringSize?: number;
    /** Per-subscriber queue size. Overflow drops oldest. Default 4096. */
    subscriberQueueSize?: number;
    /** GC delay after done() AND zero subscribers. Default 60_000. */
    gcDelayMs?: number;
    /**
     * Idle-reap backstop. A channel with ZERO subscribers and no publish/access
     * for this long is dropped even if done() was never called — the safety net
     * for a producer that died WITHOUT terminating its channel (e.g. a crashed
     * subprocess that never sent its final chunk). scheduleGC only handles the
     * normal done()+0-subs case (gcDelayMs); this catches the never-done case,
     * which scheduleGC's `!isDone` guard otherwise skips forever. Default 600_000
     * (10 min).
     */
    idleReapMs?: number;
}
export declare function getChannel<T>(key: string, opts?: ChannelOptions): BusChannel<T>;
export declare function dropChannel(key: string): boolean;
export declare function listChannels(): Array<{
    key: string;
    subscribers: number;
    syncHandlers: number;
    isDone: boolean;
    recentCount: number;
}>;
/** Test-only. Clears the global registry so each test starts clean. */
export declare function _resetChannelsForTest(): void;
