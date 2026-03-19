const queue_mod = @import("concurrent-queue");
const ConcurrentQueue = queue_mod.ConcurrentQueue;
const std = @import("std");

const Q = ConcurrentQueue(u64, .{});

export fn zig_enqueue_one(q: *Q, ptok: *Q.ProducerToken) callconv(.C) i32 {
    q.enqueue(ptok, 42) catch return -1;
    return 0;
}

export fn zig_try_dequeue_one(q: *Q, ctok: *Q.ConsumerToken, out: *u64) callconv(.C) i32 {
    if (q.tryDequeue(ctok)) |val| {
        out.* = val;
        return 1;
    }
    return 0;
}

export fn zig_try_dequeue_any(q: *Q, out: *u64) callconv(.C) i32 {
    if (q.tryDequeueAny()) |val| {
        out.* = val;
        return 1;
    }
    return 0;
}
