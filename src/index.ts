// Public API — server bits (framework-free).

// Server
export { sseResponse, parseLastEventId, bridgeChannel } from './server/response';
export type { SseSink, SseResponseOptions } from './server/response';

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
export { createResilientEventSource } from './client/resilient-event-source';
export type {
  ResilientEventSource,
  ResilientEventSourceOptions,
  ResilientEventSourceStatus,
} from './client/resilient-event-source';
export { parseSseStream } from './client/parse-stream';
export type { ParsedSseEvent } from './client/parse-stream';

// Vocabulary
export type { SyncSseEventVocabulary } from './sync-events';
