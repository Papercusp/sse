/**
 * Node/Express bridge for `sseResponse()`.
 *
 * `sseResponse()` returns a Web `Response` (the Web-standard contract used by
 * Next.js / Hono / Workers). Server frameworks built on Node's http layer
 * (NestJS, Express) instead hand you a Node `ServerResponse` to write into.
 * This helper bridges the two so the *server* transport is shared, not just the
 * wire format + client.
 *
 * Deps-free on purpose: it pumps the Web `ReadableStream` body with the
 * Web-standard reader API and writes to a minimal structural response surface —
 * no `node:stream` import, so the package stays dependency-free and Web-first.
 */

/**
 * The slice of a Node `http.ServerResponse` this bridge needs. Express's `res`
 * (which extends `ServerResponse`) satisfies it structurally.
 */
export interface NodeResponseLike {
  statusCode: number;
  setHeader(name: string, value: string): void;
  write(chunk: Uint8Array): boolean;
  end(): void;
  /** Optional: used to tear the socket down on a mid-stream read error. */
  destroy?(err?: Error): void;
}

/**
 * Copy a Web `Response` (from `sseResponse()`) onto a Node/Express response:
 * status + headers (+ any `extraHeaders`), then stream the body. Returns
 * immediately — the body is pumped in the background, matching the
 * `Readable.fromWeb(res.body).pipe(res)` pattern callers hand-rolled before.
 *
 * @example
 *   const webRes = sseResponse<MyEvents>({ ... });
 *   sseResponseToNode(webRes, res, { 'X-Turn-Id': turnId });
 */
export function sseResponseToNode(
  webRes: Response,
  res: NodeResponseLike,
  extraHeaders?: Record<string, string>,
): void {
  res.statusCode = webRes.status;
  webRes.headers.forEach((value, key) => res.setHeader(key, value));
  if (extraHeaders) {
    for (const [key, value] of Object.entries(extraHeaders)) res.setHeader(key, value);
  }
  const body = webRes.body;
  if (!body) {
    res.end();
    return;
  }
  void pumpWebBodyToNode(body, res);
}

async function pumpWebBodyToNode(
  body: ReadableStream<Uint8Array>,
  res: NodeResponseLike,
): Promise<void> {
  const reader = body.getReader();
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value) res.write(value);
    }
    res.end();
  } catch (err) {
    if (res.destroy) res.destroy(err instanceof Error ? err : new Error(String(err)));
    else res.end();
  }
}
