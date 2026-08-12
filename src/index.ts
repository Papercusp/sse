// Public API — server bits (framework-free).

// Server
export { sseResponse, parseLastEventId, bridgeChannel } from './server/response';
export type { SseSink, SseResponseOptions } from './server/response';

// Node/Express bridge for `sseResponse()` (deps-free; structural Node response).
export { sseResponseToNode } from './server/node-bridge';
export type { NodeResponseLike } from './server/node-bridge';

// Channel
export {
  getChannel,
  dropChannel,
  listChannels,
  _resetChannelsForTest,
} from './server/channel';
export type { BusChannel, ChannelOptions } from './server/channel';

// Max-duration constants
export { SYNC_MAX_DURATION_SEC, ACTION_MAX_DURATION_SEC } from './server/max-duration';

// Wire format (low-level; exported for advanced consumers + the snapshot test)
export { encodeFrame, heartbeatFrame, commentFrame } from './wire/format';
export type { SseFrame } from './wire/format';

// Client primitives (framework-free)
export {
  createResilientEventSource,
  // A CEILING for any consumer running its own silence timer — see the constant.
  DEFAULT_ZOMBIE_TIMEOUT_MS,
} from './client/resilient-event-source';
// Live standing-stream registry (per-host connection budget — see stream-registry).
export {
  listLiveStreams,
  countLiveStreamsForHost,
  registerLiveStream,
  STREAM_BUDGET_WARN_AT,
} from './client/stream-registry';
export type { LiveStreamRecord } from './client/stream-registry';
export type {
  ResilientEventSource,
  ResilientEventSourceOptions,
  ResilientEventSourceStatus,
} from './client/resilient-event-source';
export { parseSseStream } from './client/parse-stream';
export type { ParsedSseEvent } from './client/parse-stream';
export { resilientPostStream } from './client/resilient-post-stream';
export type {
  ResilientPostStreamOptions,
  ResilientPostResume,
  ResilientStreamEvent,
} from './client/resilient-post-stream';

// Vocabulary
export type { SyncSseEventVocabulary } from './sync-events';
