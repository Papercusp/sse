"use strict";
/**
 * parseSseStream — async iterable of ParsedSseEvent over a fetch response body.
 *
 * For the POST + body.getReader() case where EventSource isn't usable (no
 * way to send a request body). Wraps the vendored spec-compliant parser.
 *
 * Handles partial UTF-8 sequences across chunk boundaries via TextDecoder's
 * stream:true mode.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.parseSseStream = parseSseStream;
const eventsource_parser_1 = require("../_vendor/eventsource-parser");
async function* parseSseStream(body) {
    if (!body)
        return;
    const reader = body.getReader();
    const decoder = new TextDecoder('utf-8'); // handles BOM + multi-byte splits in stream mode
    const parser = (0, eventsource_parser_1.createParser)();
    try {
        while (true) {
            const { done, value } = await reader.read();
            if (done) {
                // Drain any final line by feeding a terminator.
                const tail = decoder.decode(new Uint8Array(), { stream: false });
                for (const ev of parser.feed(tail + '\n'))
                    yield ev;
                return;
            }
            const text = decoder.decode(value, { stream: true });
            for (const ev of parser.feed(text))
                yield ev;
        }
    }
    finally {
        try {
            reader.releaseLock();
        }
        catch { /* ignore */ }
    }
}
