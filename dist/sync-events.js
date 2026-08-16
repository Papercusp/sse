"use strict";
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
Object.defineProperty(exports, "__esModule", { value: true });
