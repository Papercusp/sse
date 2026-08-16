/**
 * SSE wire-format primitives.
 *
 * Encodes a single SSE frame to its on-the-wire bytes per the spec at
 * https://html.spec.whatwg.org/multipage/server-sent-events.html. The exact
 * byte layout is a STABILITY CONTRACT — see README.md "Wire-format stability
 * contract" + `wire-format-snapshot.test.ts`. Changing anything in this file
 * requires a coordinated migration of every shipped desktop client.
 */
export interface SseFrame {
    /** Optional event id. When present, sent as `id: <value>` line. */
    id?: number | string;
    /** Optional event name. When omitted or 'message', the `event:` line is suppressed (spec default). */
    event?: string;
    /** Required payload. May contain \n; we emit one `data:` line per split. */
    data: string;
    /** Optional reconnect retry override (ms). Almost always absent — the resilient client manages its own backoff. */
    retry?: number;
}
/**
 * Serialize a frame to its UTF-8 bytes. Always terminated by `\n\n`.
 *
 * Field order is fixed: id → event → data → retry. The spec doesn't mandate
 * order but consumers (and the snapshot test) depend on this ordering.
 */
export declare function encodeFrame(frame: SseFrame): Uint8Array;
/**
 * Build a heartbeat frame: `event: heartbeat\ndata: {"tsMs":<now>}\n\n`. No
 * id — heartbeats are not part of the resumable event log.
 */
export declare function heartbeatFrame(now?: number): Uint8Array;
/**
 * Build a comment line: `: <text>\n\n`. Invisible to EventSource listeners.
 * Use sparingly; prefer `heartbeatFrame` for keepalive.
 */
export declare function commentFrame(text: string): Uint8Array;
