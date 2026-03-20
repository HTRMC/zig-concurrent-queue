const std = @import("std");
const queue_mod = @import("concurrent-queue");
const Q = queue_mod.ConcurrentQueue(u64, .{});
const EP = Q.ExplicitProducer;
const PB = Q.ProducerBase;
const Block = Q.Block;

pub fn main() !void {
    var buf: [4096]u8 = undefined;
    const w = std.fs.File.stdout().writer(&buf);

    try w.print("=== Zig struct sizes ===\n", .{});
    try w.print("ConcurrentQueue:    {d} bytes\n", .{@sizeOf(Q)});
    try w.print("ProducerBase:       {d} bytes\n", .{@sizeOf(PB)});
    try w.print("ExplicitProducer:   {d} bytes\n", .{@sizeOf(EP)});
    try w.print("Block:              {d} bytes\n", .{@sizeOf(Block)});
    try w.print("ProducerToken:      {d} bytes\n", .{@sizeOf(Q.ProducerToken)});
    try w.print("ConsumerToken:      {d} bytes\n\n", .{@sizeOf(Q.ConsumerToken)});

    try w.print("=== ProducerBase layout (cache line = 64 bytes) ===\n", .{});
    const pb_fields = .{
        .{ "tail_index", @offsetOf(PB, "tail_index") },
        .{ "head_index", @offsetOf(PB, "head_index") },
        .{ "dequeue_optimistic_count", @offsetOf(PB, "dequeue_optimistic_count") },
        .{ "dequeue_overcommit", @offsetOf(PB, "dequeue_overcommit") },
        .{ "next_producer", @offsetOf(PB, "next_producer") },
        .{ "active", @offsetOf(PB, "active") },
        .{ "is_explicit", @offsetOf(PB, "is_explicit") },
    };
    inline for (pb_fields) |f| {
        try w.print("  [{d:3}] CL{d}  {s}\n", .{ f[1], f[1] / 64, f[0] });
    }

    try w.print("\n=== ExplicitProducer layout ===\n", .{});
    const ep_fields = .{
        .{ "base", @offsetOf(EP, "base") },
        .{ "tail_block", @offsetOf(EP, "tail_block") },
        .{ "block_index", @offsetOf(EP, "block_index") },
        .{ "pr_block_index_front", @offsetOf(EP, "pr_block_index_front") },
        .{ "pr_block_index_size", @offsetOf(EP, "pr_block_index_size") },
        .{ "pr_block_index_slots_used", @offsetOf(EP, "pr_block_index_slots_used") },
        .{ "parent", @offsetOf(EP, "parent") },
    };
    inline for (ep_fields) |f| {
        try w.print("  [{d:3}] CL{d}  {s}\n", .{ f[1], f[1] / 64, f[0] });
    }

    try w.print("\n=== Block layout ===\n", .{});
    try w.print("  [{d:3}] data ({d} element slots)\n", .{ @offsetOf(Block, "data"), 32 });
    try w.print("  [{d:3}] empty_flags\n", .{@offsetOf(Block, "empty_flags")});
    try w.print("  [{d:3}] elements_completely_dequeued\n", .{@offsetOf(Block, "elements_completely_dequeued")});
    try w.print("  [{d:3}] next\n", .{@offsetOf(Block, "next")});
    try w.print("  [{d:3}] free_list_refs\n", .{@offsetOf(Block, "free_list_refs")});
    try w.print("  [{d:3}] free_list_next\n", .{@offsetOf(Block, "free_list_next")});
    try w.print("  [{d:3}] dynamically_allocated\n", .{@offsetOf(Block, "dynamically_allocated")});

    try w.print("\n=== False sharing analysis ===\n", .{});
    const ti = @offsetOf(PB, "tail_index");
    const hi = @offsetOf(PB, "head_index");
    const doc = @offsetOf(PB, "dequeue_optimistic_count");
    const dov = @offsetOf(PB, "dequeue_overcommit");
    if (ti / 64 == hi / 64) {
        try w.print("  WARNING: tail_index and head_index on SAME cache line {d}\n", .{ti / 64});
        try w.print("    Producer writes tail_index, consumer writes head_index\n", .{});
        try w.print("    => false sharing on every enqueue+dequeue pair\n", .{});
    } else {
        try w.print("  OK: tail_index (CL{d}) and head_index (CL{d}) on different lines\n", .{ ti / 64, hi / 64 });
    }
    if (doc / 64 == hi / 64) {
        try w.print("  WARNING: dequeue_optimistic_count and head_index on SAME cache line\n", .{});
    }
    if (doc / 64 == ti / 64) {
        try w.print("  NOTE: dequeue_optimistic_count and tail_index on same cache line\n", .{});
        try w.print("    (both read by consumer, tail written by producer)\n", .{});
    }
    _ = dov;
}
