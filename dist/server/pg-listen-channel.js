"use strict";
/**
 * pgListenChannel — bridges PG LISTEN/NOTIFY into a BusChannel.
 *
 * The pattern used by `apps/operator/app/api/zero-harness/sse/route.ts` and
 * its inline `subscribe()` helper, generalized. Calls `sql.listen()` on a
 * postgres-js client, parses each NOTIFY payload, publishes to the
 * underlying channel.
 *
 * Caveats (documented for callers):
 *   - PG NOTIFY payload limit is 8 KB. Larger payloads → use invalidate-only
 *     + REST refetch pattern (see SyncSseEventVocabulary.invalidate).
 *   - NOTIFY is best-effort: messages sent during a transient PG
 *     disconnect are LOST. Callers needing durable delivery should pair
 *     this with a WAL/event-log table + the `backfill` callback.
 *   - parse() errors are swallowed (logged via console.warn), not thrown —
 *     a bad payload should not tear down the whole listener.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.pgListenChannel = pgListenChannel;
const channel_1 = require("./channel");
async function pgListenChannel(opts) {
    const parse = opts.parse ?? ((s) => JSON.parse(s));
    const key = opts.channelKey ?? `pg-listen:${opts.channel}`;
    const ringSize = opts.ringSize ?? 1000;
    const channel = (0, channel_1.getChannel)(key, { ...opts, ringSize });
    // postgres-js sql.listen() returns an object with `unlisten()`. The
    // callback receives the raw payload string.
    const handle = await opts.sql.listen(opts.channel, (payload) => {
        try {
            const event = parse(payload);
            channel.publish(event);
        }
        catch (err) {
            // Bad payload — log + drop. Don't crash the listener.
            // eslint-disable-next-line no-console
            console.warn(`[pgListenChannel] parse error on channel ${opts.channel}:`, err instanceof Error ? err.message : err);
        }
    });
    let unlistened = false;
    return {
        publish: channel.publish.bind(channel),
        done: channel.done.bind(channel),
        get isDone() { return channel.isDone; },
        get donePayload() { return channel.donePayload; },
        get recent() { return channel.recent; },
        recentSince: channel.recentSince.bind(channel),
        subscribe: channel.subscribe.bind(channel),
        get subscriberCount() { return channel.subscriberCount; },
        onPublish: channel.onPublish.bind(channel),
        onDone: channel.onDone.bind(channel),
        get syncHandlerCount() { return channel.syncHandlerCount; },
        async unlisten() {
            if (unlistened)
                return;
            unlistened = true;
            try {
                await handle.unlisten();
            }
            catch { /* listener already gone */ }
            channel.done({ reason: 'unlisten' });
        },
        async backfillSince(sinceId) {
            if (!opts.backfill)
                return [];
            try {
                return await opts.backfill(sinceId);
            }
            catch (err) {
                // eslint-disable-next-line no-console
                console.warn(`[pgListenChannel] backfill error on channel ${opts.channel}:`, err instanceof Error ? err.message : err);
                return [];
            }
        },
    };
}
