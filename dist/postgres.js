"use strict";
// Postgres peer entry. Requires `postgres` (postgres-js) as a peer dep.
Object.defineProperty(exports, "__esModule", { value: true });
exports.pgListenChannel = void 0;
var pg_listen_channel_1 = require("./server/pg-listen-channel");
Object.defineProperty(exports, "pgListenChannel", { enumerable: true, get: function () { return pg_listen_channel_1.pgListenChannel; } });
