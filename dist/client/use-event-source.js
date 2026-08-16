"use strict";
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
'use client';
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.useEventSource = useEventSource;
const react_1 = require("react");
const resilient_event_source_1 = require("./resilient-event-source");
function useEventSource(url, opts) {
    const [status, setStatus] = (0, react_1.useState)('idle');
    const [lastEventId, setLastEventId] = (0, react_1.useState)(null);
    const sourceRef = (0, react_1.useRef)(null);
    // Keep handlers + callbacks in refs so they don't trigger re-subscription.
    const optsRef = (0, react_1.useRef)(opts);
    optsRef.current = opts;
    (0, react_1.useEffect)(() => {
        if (!url) {
            setStatus('idle');
            return;
        }
        const source = (0, resilient_event_source_1.createResilientEventSource)({
            ...optsRef.current,
            url,
            // Wrap lifecycle callbacks to keep our state in sync AND forward to caller.
            onStatusChange: (s) => {
                setStatus(s);
                optsRef.current.onStatusChange?.(s);
            },
            onOpen: () => {
                optsRef.current.onOpen?.();
            },
            onError: (err) => {
                optsRef.current.onError?.(err);
            },
            // Wrap each user handler so we capture lastEventId without forcing
            // the caller to do it themselves. Wrapper looks up the latest handler
            // from optsRef each time, so re-renders with new handler closures
            // work without re-subscription.
            handlers: Object.fromEntries(Object.keys(optsRef.current.handlers).map((name) => [
                name,
                (data, ev) => {
                    if (ev.lastEventId)
                        setLastEventId(ev.lastEventId);
                    optsRef.current.handlers[name]?.(data, ev);
                },
            ])),
        });
        sourceRef.current = source;
        return () => {
            source.close();
            sourceRef.current = null;
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [url, opts.withCredentials]);
    const reconnect = (0, react_1.useCallback)(() => {
        sourceRef.current?.reconnect();
    }, []);
    const close = (0, react_1.useCallback)(() => {
        sourceRef.current?.close();
    }, []);
    return { status, lastEventId, reconnect, close };
}
