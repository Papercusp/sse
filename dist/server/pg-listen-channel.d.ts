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
import type { Sql } from 'postgres';
import { type BusChannel, type ChannelOptions } from './channel';
export interface PgListenChannelOptions<T> extends ChannelOptions {
    /** postgres-js client (the one returned by getOrgPg().sql or similar). */
    sql: Sql;
    /** PG NOTIFY channel name. Routed to a BusChannel keyed `pg-listen:<channel>`. */
    channel: string;
    /** Decode the NOTIFY payload string. Defaults to JSON.parse. */
    parse?: (payload: string) => T;
    /**
     * Optional persistent backfill for Last-Event-ID resume beyond the
     * in-memory ring buffer. Items returned are NOT published — they're
     * exposed via `backfillSince(sinceId)` for `sseResponse({ replay })`.
     */
    backfill?: (sinceId: number) => Promise<Array<{
        id: number;
        event: T;
    }>>;
    /** Override the channel key. Default is `pg-listen:<channel>`. */
    channelKey?: string;
}
export interface PgBusChannel<T> extends BusChannel<T> {
    /** Stop LISTEN'ing on PG; channel goes done and is GC'd. Idempotent. */
    unlisten(): Promise<void>;
    /**
     * If `backfill` was provided, return persisted events with id > sinceId.
     * Returns [] if no backfill function or sinceId >= maxKnownId.
     */
    backfillSince(sinceId: number): Promise<Array<{
        id: number;
        event: T;
    }>>;
}
export declare function pgListenChannel<T>(opts: PgListenChannelOptions<T>): Promise<PgBusChannel<T>>;
