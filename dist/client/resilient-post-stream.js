"use strict";
/**
 * resilientPostStream — reconnect-safe SSE over a POST request.
 *
 * `EventSource` (and so `createResilientEventSource`) can only GET, but many
 * streams need a request body (a chat turn, a query). This is the POST-capable
 * sibling: it POSTs, reads the response body with {@link parseSseStream}, and —
 * if the connection drops before a terminal event — RESUMES the same logical
 * stream with `Last-Event-ID` + a caller-built resume body, under bounded
 * exponential backoff. The server is expected to keep the turn alive and replay
 * missed events on resume (the prospector reconnect-safe protocol).
 *
 * Yields each JSON-decoded event payload; stops after the first terminal event.
 * Non-JSON `data`, empty `data`, and `skipEvents` (default: heartbeat) are
 * dropped. Generalizes the hand-rolled loop that lived in RecommenderChat.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.resilientPostStream = resilientPostStream;
const parse_stream_1 = require("./parse-stream");
const defaultDelay = (ms) => new Promise((r) => setTimeout(r, ms));
async function* resilientPostStream(opts) {
    const { url, buildBody, isTerminal, turnIdHeader, headers = {}, maxResumeAttempts = 5, backoffBaseMs = 400, backoffCapMs = 4000, skipEvents = ['heartbeat'], signal, fetchImpl = fetch, delayImpl = defaultDelay, } = opts;
    let turnId = null;
    let lastEventId = 0;
    let completed = false;
    async function* openOnce(resume) {
        const res = await fetchImpl(url, {
            method: 'POST',
            signal,
            headers: {
                'Content-Type': 'application/json',
                ...headers,
                ...(resume && lastEventId ? { 'Last-Event-ID': String(lastEventId) } : {}),
            },
            body: JSON.stringify(buildBody(resume ? { turnId, lastEventId } : null)),
        });
        if (!res.ok || !res.body)
            throw new Error(`HTTP ${res.status}`);
        if (turnIdHeader)
            turnId = res.headers.get(turnIdHeader) || turnId;
        try {
            for await (const ev of (0, parse_stream_1.parseSseStream)(res.body)) {
                if (ev.event && skipEvents.includes(ev.event))
                    continue;
                if (ev.id != null && ev.id !== '')
                    lastEventId = Math.max(lastEventId, Number(ev.id));
                if (!ev.data)
                    continue;
                let data;
                try {
                    data = JSON.parse(ev.data);
                }
                catch {
                    continue;
                }
                const terminal = isTerminal(data);
                yield { data, id: ev.id, event: ev.event, terminal };
                // Terminal event ends the turn. The reconnect-safe server keeps the SSE
                // connection OPEN after it (so a dropped client can resume), so stop
                // explicitly — else this read loop awaits the next event forever.
                if (terminal) {
                    completed = true;
                    return;
                }
            }
        }
        finally {
            // Stop reading ⇒ close the still-open connection, else each finished turn
            // leaks an open socket and the per-host pool starves later requests.
            try {
                await res.body?.cancel();
            }
            catch { /* already closing */ }
        }
    }
    yield* openOnce(false);
    // Stream ended without a terminal event ⇒ the connection dropped. Resume the
    // same turn from where we left off, with bounded backoff.
    for (let attempt = 1; !completed && turnId && attempt <= maxResumeAttempts; attempt++) {
        if (signal?.aborted)
            return;
        await delayImpl(Math.min(backoffBaseMs * 2 ** (attempt - 1), backoffCapMs));
        try {
            yield* openOnce(true);
        }
        catch {
            /* keep retrying until the attempt budget is spent */
        }
    }
}
