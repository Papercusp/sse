/**
 * Minimal spec-compliant SSE parser.
 *
 * Implements the wire-format consumer side of
 * https://html.spec.whatwg.org/multipage/server-sent-events.html#parsing-an-event-stream
 *
 * Spec'd behaviors:
 *   - Field names: id, event, data, retry (others ignored)
 *   - Newlines: \r\n, \r, \n all accepted as separators
 *   - Comments: lines starting with ':' are dropped
 *   - Multi-line data: each `data:` line concatenated with \n
 *   - Empty line dispatches the buffered event
 *   - Stripped leading space after the colon (only one)
 *   - BOM at stream start is consumed
 *
 * Vendored rather than depending on `eventsource-parser` (npm) because the
 * production sync path treats wire-format as a stability contract and we
 * want full control over the parser. ~80 lines, MIT-licensed equivalent.
 */
export interface ParserEvent {
    id?: string;
    event: string;
    data: string;
    retry?: number;
}
export interface Parser {
    /** Feed UTF-8 text. Yields zero or more dispatched events. */
    feed(chunk: string): ParserEvent[];
    /** Force any pending partial line to be discarded. */
    reset(): void;
}
export declare function createParser(): Parser;
