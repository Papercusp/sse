/**
 * Typed event vocabulary for desktop Zero-sync over SSE.
 *
 * The wire format here is a STABILITY CONTRACT — see README.md and
 * wire-format-snapshot.test.ts. Don't add or rename fields without a
 * coordinated migration of every shipped desktop client.
 *
 * Producers writing to the desktop sync channel should pass this as the
 * generic to `sseResponse<SyncSseEventVocabulary<TPayload>>(...)` so
 * typos and shape mismatches fail at compile time.
 */

export type SyncSseEventVocabulary<TPayload = unknown> = {
  /** Push: server sends the full payload; client setQueryData directly. */
  update: { name: string; args?: unknown; data: TPayload; tsMs?: number };
  /** Invalidate: server notifies a queryName is stale; client refetches via REST. */
  invalidate: { name: string; args?: unknown; tsMs?: number };
  /** Liveness ping. Always emitted by the lib's heartbeat timer. */
  heartbeat: { tsMs: number };
};
