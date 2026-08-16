"use strict";
/**
 * Monotonic id allocator.
 *
 * Used by `getChannel()` and `sseResponse()` to assign auto-increment ids to
 * events that don't carry an explicit id. Process-local state — ids reset on
 * operator restart. For durable ids across restart (Last-Event-ID resumption
 * past a restart), callers should seed via `setFloor()` from a persistent
 * source like a PG sequence — see `pgListenChannel`.
 *
 * Not exported as a public API; consumed internally by channel.ts and
 * response.ts.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.createIdAllocator = createIdAllocator;
function createIdAllocator(initial = 0) {
    let floor = initial;
    return {
        next() {
            floor += 1;
            return floor;
        },
        get floor() {
            return floor;
        },
        setFloor(value) {
            if (value > floor)
                floor = value;
        },
    };
}
