/**
 * parseSseStream — async iterable of ParsedSseEvent over a fetch response body.
 *
 * For the POST + body.getReader() case where EventSource isn't usable (no
 * way to send a request body). Wraps the vendored spec-compliant parser.
 *
 * Handles partial UTF-8 sequences across chunk boundaries via TextDecoder's
 * stream:true mode.
 */
import { type ParserEvent } from '../_vendor/eventsource-parser';
export type ParsedSseEvent = ParserEvent;
export declare function parseSseStream(body: ReadableStream<Uint8Array> | null): AsyncGenerator<ParsedSseEvent, void, unknown>;
