"use strict";
// Public API — server bits (framework-free).
Object.defineProperty(exports, "__esModule", { value: true });
exports.resilientPostStream = exports.parseSseStream = exports.STREAM_BUDGET_WARN_AT = exports.registerLiveStream = exports.countLiveStreamsForHost = exports.listLiveStreams = exports.DESKTOP_IPC_STREAM_IDLE_TIMEOUT_MS = exports.DEFAULT_ZOMBIE_TIMEOUT_MS = exports.createResilientEventSource = exports.commentFrame = exports.heartbeatFrame = exports.encodeFrame = exports.ACTION_MAX_DURATION_SEC = exports.SYNC_MAX_DURATION_SEC = exports._resetChannelsForTest = exports.listChannels = exports.dropChannel = exports.getChannel = exports.sseResponseToNode = exports.bridgeChannel = exports.parseLastEventId = exports.sseResponse = void 0;
// Server
var response_1 = require("./server/response");
Object.defineProperty(exports, "sseResponse", { enumerable: true, get: function () { return response_1.sseResponse; } });
Object.defineProperty(exports, "parseLastEventId", { enumerable: true, get: function () { return response_1.parseLastEventId; } });
Object.defineProperty(exports, "bridgeChannel", { enumerable: true, get: function () { return response_1.bridgeChannel; } });
// Node/Express bridge for `sseResponse()` (deps-free; structural Node response).
var node_bridge_1 = require("./server/node-bridge");
Object.defineProperty(exports, "sseResponseToNode", { enumerable: true, get: function () { return node_bridge_1.sseResponseToNode; } });
// Channel
var channel_1 = require("./server/channel");
Object.defineProperty(exports, "getChannel", { enumerable: true, get: function () { return channel_1.getChannel; } });
Object.defineProperty(exports, "dropChannel", { enumerable: true, get: function () { return channel_1.dropChannel; } });
Object.defineProperty(exports, "listChannels", { enumerable: true, get: function () { return channel_1.listChannels; } });
Object.defineProperty(exports, "_resetChannelsForTest", { enumerable: true, get: function () { return channel_1._resetChannelsForTest; } });
// Max-duration constants
var max_duration_1 = require("./server/max-duration");
Object.defineProperty(exports, "SYNC_MAX_DURATION_SEC", { enumerable: true, get: function () { return max_duration_1.SYNC_MAX_DURATION_SEC; } });
Object.defineProperty(exports, "ACTION_MAX_DURATION_SEC", { enumerable: true, get: function () { return max_duration_1.ACTION_MAX_DURATION_SEC; } });
// Wire format (low-level; exported for advanced consumers + the snapshot test)
var format_1 = require("./wire/format");
Object.defineProperty(exports, "encodeFrame", { enumerable: true, get: function () { return format_1.encodeFrame; } });
Object.defineProperty(exports, "heartbeatFrame", { enumerable: true, get: function () { return format_1.heartbeatFrame; } });
Object.defineProperty(exports, "commentFrame", { enumerable: true, get: function () { return format_1.commentFrame; } });
// Client primitives (framework-free)
var resilient_event_source_1 = require("./client/resilient-event-source");
Object.defineProperty(exports, "createResilientEventSource", { enumerable: true, get: function () { return resilient_event_source_1.createResilientEventSource; } });
// A CEILING for any consumer running its own silence timer — see the constant.
Object.defineProperty(exports, "DEFAULT_ZOMBIE_TIMEOUT_MS", { enumerable: true, get: function () { return resilient_event_source_1.DEFAULT_ZOMBIE_TIMEOUT_MS; } });
// Desktop endpoint-IPC has a tighter stream ceiling than the generic wrapper.
Object.defineProperty(exports, "DESKTOP_IPC_STREAM_IDLE_TIMEOUT_MS", { enumerable: true, get: function () { return resilient_event_source_1.DESKTOP_IPC_STREAM_IDLE_TIMEOUT_MS; } });
// Live standing-stream registry (per-host connection budget — see stream-registry).
var stream_registry_1 = require("./client/stream-registry");
Object.defineProperty(exports, "listLiveStreams", { enumerable: true, get: function () { return stream_registry_1.listLiveStreams; } });
Object.defineProperty(exports, "countLiveStreamsForHost", { enumerable: true, get: function () { return stream_registry_1.countLiveStreamsForHost; } });
Object.defineProperty(exports, "registerLiveStream", { enumerable: true, get: function () { return stream_registry_1.registerLiveStream; } });
Object.defineProperty(exports, "STREAM_BUDGET_WARN_AT", { enumerable: true, get: function () { return stream_registry_1.STREAM_BUDGET_WARN_AT; } });
var parse_stream_1 = require("./client/parse-stream");
Object.defineProperty(exports, "parseSseStream", { enumerable: true, get: function () { return parse_stream_1.parseSseStream; } });
var resilient_post_stream_1 = require("./client/resilient-post-stream");
Object.defineProperty(exports, "resilientPostStream", { enumerable: true, get: function () { return resilient_post_stream_1.resilientPostStream; } });
