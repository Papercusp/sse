/**
 * useEventSource — React hook wrapping createResilientEventSource.
 *
 * Subscribes to a server-sent-events stream with all the resilience
 * primitives of createResilientEventSource: backoff with jitter, zombie
 * watchdog, escalation on consecutive failures, optional visibility pause.
 *
 * Pass `url=null` to disable (component renders but no connection opens).
 * When `url` changes, the hook cleanly closes the old source and opens a
 * new one.
 *
 * Handlers are kept in a ref so callers don't have to memoize them — only
 * `url` and `withCredentials` cause re-subscription. Lifecycle callbacks
 * (onOpen, onStatusChange, onError) follow refs too.
 */
import { type ResilientEventSourceOptions, type ResilientEventSourceStatus } from './resilient-event-source';
export type { ResilientEventSourceStatus } from './resilient-event-source';
export interface UseEventSourceOptions extends Omit<ResilientEventSourceOptions, 'url'> {
}
export interface UseEventSourceResult {
    status: ResilientEventSourceStatus;
    lastEventId: string | null;
    reconnect: () => void;
    close: () => void;
}
export declare function useEventSource(url: string | null, opts: UseEventSourceOptions): UseEventSourceResult;
