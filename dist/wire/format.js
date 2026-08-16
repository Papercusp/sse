"use strict";
/**
 * SSE wire-format primitives.
 *
 * Encodes a single SSE frame to its on-the-wire bytes per the spec at
 * https://html.spec.whatwg.org/multipage/server-sent-events.html. The exact
 * byte layout is a STABILITY CONTRACT — see README.md "Wire-format stability
 * contract" + `wire-format-snapshot.test.ts`. Changing anything in this file
 * requires a coordinated migration of every shipped desktop client.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.encodeFrame = encodeFrame;
exports.heartbeatFrame = heartbeatFrame;
exports.commentFrame = commentFrame;
const TEXT_ENCODER = new TextEncoder();
/**
 * Serialize a frame to its UTF-8 bytes. Always terminated by `\n\n`.
 *
 * Field order is fixed: id → event → data → retry. The spec doesn't mandate
 * order but consumers (and the snapshot test) depend on this ordering.
 */
function encodeFrame(frame) {
    let out = '';
    if (frame.id !== undefined) {
        out += `id: ${frame.id}\n`;
    }
    if (frame.event !== undefined && frame.event !== 'message') {
        out += `event: ${frame.event}\n`;
    }
    // Per spec: each \n in data → new `data:` line. Empty string → one
    // empty data: line (consumers see an empty data string).
    const dataLines = frame.data.length === 0 ? [''] : frame.data.split('\n');
    for (const line of dataLines) {
        out += `data: ${line}\n`;
    }
    if (frame.retry !== undefined) {
        out += `retry: ${frame.retry}\n`;
    }
    out += '\n';
    return TEXT_ENCODER.encode(out);
}
/**
 * Build a heartbeat frame: `event: heartbeat\ndata: {"tsMs":<now>}\n\n`. No
 * id — heartbeats are not part of the resumable event log.
 */
function heartbeatFrame(now = Date.now()) {
    return encodeFrame({ event: 'heartbeat', data: JSON.stringify({ tsMs: now }) });
}
/**
 * Build a comment line: `: <text>\n\n`. Invisible to EventSource listeners.
 * Use sparingly; prefer `heartbeatFrame` for keepalive.
 */
function commentFrame(text) {
    return TEXT_ENCODER.encode(`: ${text}\n\n`);
}
