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
export interface ResilientPostResume {
    /** Resumable turn id from the response header (see `turnIdHeader`), or null. */
    turnId: string | null;
    /** Highest SSE event id seen so far (sent as Last-Event-ID on resume). */
    lastEventId: number;
}
export interface ResilientPostStreamOptions {
    url: string;
    /** Build the request body. `resume` is null on the first attempt, else the
     *  cursor for a reconnect — return the resume payload (e.g. `{ turnId, lastEventId }`). */
    buildBody: (resume: ResilientPostResume | null) => unknown;
    /** True when a decoded payload is terminal (ends the stream — e.g. `done`/`error`). */
    isTerminal: (data: unknown) => boolean;
    /** Response header carrying the resumable turn id (e.g. `X-Prospector-Turn-Id`). */
    turnIdHeader?: string;
    /** Extra request headers (Content-Type: application/json is always set). */
    headers?: Record<string, string>;
    /** Max resume attempts after the first stream drops. Default 5. */
    maxResumeAttempts?: number;
    /** Backoff ms = min(base * 2^(attempt-1), cap). Defaults: base 400, cap 4000. */
    backoffBaseMs?: number;
    backoffCapMs?: number;
    /** SSE event names to drop (not yielded). Default `['heartbeat']`. */
    skipEvents?: string[];
    /** Abort the stream (and skip pending resumes). */
    signal?: AbortSignal;
    /** Injectable for tests; defaults to global fetch. */
    fetchImpl?: typeof fetch;
    /** Injectable for tests; defaults to setTimeout-based delay. */
    delayImpl?: (ms: number) => Promise<void>;
}
export interface ResilientStreamEvent<T = unknown> {
    /** JSON-decoded `data`. */
    data: T;
    id?: number | string;
    event?: string;
    /** True for the terminal event that ends the stream. */
    terminal: boolean;
}
export declare function resilientPostStream<T = unknown>(opts: ResilientPostStreamOptions): AsyncGenerator<ResilientStreamEvent<T>, void, unknown>;
