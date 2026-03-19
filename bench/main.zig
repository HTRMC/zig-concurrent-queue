const std = @import("std");
const ConcurrentQueue = @import("concurrent-queue").ConcurrentQueue;

const NUM_PRODUCERS = 4;
const NUM_CONSUMERS = 4;
const ITEMS_PER_PRODUCER = 1_000_000;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const Q = ConcurrentQueue(u64, .{});
    var queue = Q.init(allocator);
    defer queue.deinit();

    const stdout = std.io.getStdOut().writer();
    try stdout.print("zig-concurrent-queue benchmark\n", .{});
    try stdout.print("  producers: {d}  consumers: {d}  items/producer: {d}\n\n", .{
        NUM_PRODUCERS, NUM_CONSUMERS, ITEMS_PER_PRODUCER,
    });

    var total_dequeued = std.atomic.Value(usize).init(0);
    var done = std.atomic.Value(bool).init(false);

    var producer_threads: [NUM_PRODUCERS]std.Thread = undefined;
    var consumer_threads: [NUM_CONSUMERS]std.Thread = undefined;

    const timer = try std.time.Timer.start();

    for (&producer_threads) |*t| {
        t.* = try std.Thread.spawn(.{}, struct {
            fn run(q: *Q) void {
                var tok = q.makeProducerToken() catch return;
                defer tok.deinit();
                for (0..ITEMS_PER_PRODUCER) |i| {
                    q.enqueue(&tok, @intCast(i)) catch return;
                }
            }
        }.run, .{&queue});
    }

    for (&consumer_threads) |*t| {
        t.* = try std.Thread.spawn(.{}, struct {
            fn run(q: *Q, dequeued: *std.atomic.Value(usize), finished: *std.atomic.Value(bool)) void {
                var local: usize = 0;
                while (!finished.load(.acquire) or q.tryDequeue() != null) {
                    if (q.tryDequeue()) |_| {
                        local += 1;
                    } else {
                        std.atomic.spinLoopHint();
                    }
                }
                _ = dequeued.fetchAdd(local, .monotonic);
            }
        }.run, .{ &queue, &total_dequeued, &done });
    }

    for (&producer_threads) |*t| t.join();
    done.store(true, .release);

    for (&consumer_threads) |*t| t.join();

    while (queue.tryDequeue()) |_| {
        _ = total_dequeued.fetchAdd(1, .monotonic);
    }

    const elapsed_ns = timer.read();
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    const total_items = NUM_PRODUCERS * ITEMS_PER_PRODUCER;
    const ops_per_sec = @as(f64, @floatFromInt(total_items)) / (@as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0);

    try stdout.print("results:\n", .{});
    try stdout.print("  total items:    {d}\n", .{total_items});
    try stdout.print("  dequeued:       {d}\n", .{total_dequeued.load(.monotonic)});
    try stdout.print("  elapsed:        {d:.2} ms\n", .{elapsed_ms});
    try stdout.print("  throughput:     {d:.0} ops/sec\n", .{ops_per_sec});
}
