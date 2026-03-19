const std = @import("std");
const ConcurrentQueue = @import("concurrent-queue").ConcurrentQueue;
const BlockingConcurrentQueue = @import("concurrent-queue").BlockingConcurrentQueue;

const NUM_PRODUCERS = 4;
const NUM_CONSUMERS = 4;
const ITEMS_PER_PRODUCER = 1_000_000;

fn benchNonBlocking(allocator: std.mem.Allocator, stdout: anytype) !void {
    const Q = ConcurrentQueue(u64, .{});
    var queue = try Q.initWithCapacity(allocator, 1024);
    defer queue.deinit();

    var total_dequeued = std.atomic.Value(usize).init(0);
    var producers_done = std.atomic.Value(usize).init(0);

    var producer_threads: [NUM_PRODUCERS]std.Thread = undefined;
    var consumer_threads: [NUM_CONSUMERS]std.Thread = undefined;

    var timer = try std.time.Timer.start();

    for (&producer_threads) |*t| {
        t.* = try std.Thread.spawn(.{}, struct {
            fn run(q: *Q, done: *std.atomic.Value(usize)) void {
                var tok = q.makeProducerToken() catch return;
                defer tok.deinit();
                for (0..ITEMS_PER_PRODUCER) |i| q.enqueue(&tok, @intCast(i)) catch return;
                _ = done.fetchAdd(1, .release);
            }
        }.run, .{ &queue, &producers_done });
    }

    for (&consumer_threads) |*t| {
        t.* = try std.Thread.spawn(.{}, struct {
            fn run(q: *Q, dequeued: *std.atomic.Value(usize), done: *std.atomic.Value(usize)) void {
                var ctok = q.makeConsumerToken();
                var local: usize = 0;
                while (true) {
                    if (q.tryDequeue(&ctok)) |_| {
                        local += 1;
                    } else {
                        if (done.load(.acquire) >= NUM_PRODUCERS) break;
                        std.atomic.spinLoopHint();
                    }
                }
                while (q.tryDequeue(&ctok)) |_| local += 1;
                _ = dequeued.fetchAdd(local, .monotonic);
            }
        }.run, .{ &queue, &total_dequeued, &producers_done });
    }

    for (&producer_threads) |*t| t.join();
    for (&consumer_threads) |*t| t.join();
    while (queue.tryDequeueAny()) |_| _ = total_dequeued.fetchAdd(1, .monotonic);

    const elapsed_ns = timer.read();
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    const total_items = NUM_PRODUCERS * ITEMS_PER_PRODUCER;
    const ops_per_sec = @as(f64, @floatFromInt(total_items)) / (@as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0);

    try stdout.print("non-blocking queue:\n", .{});
    try stdout.print("  total:      {d}\n", .{total_items});
    try stdout.print("  dequeued:   {d}\n", .{total_dequeued.load(.monotonic)});
    try stdout.print("  elapsed:    {d:.2} ms\n", .{elapsed_ms});
    try stdout.print("  throughput: {d:.0} ops/sec\n\n", .{ops_per_sec});
}

fn benchBlocking(allocator: std.mem.Allocator, stdout: anytype) !void {
    const Q = BlockingConcurrentQueue(u64, .{});
    var queue = try Q.initWithCapacity(allocator, 1024);
    defer queue.deinit();

    var producers_done = std.atomic.Value(usize).init(0);

    var producer_threads: [NUM_PRODUCERS]std.Thread = undefined;
    var consumer_threads: [NUM_CONSUMERS]std.Thread = undefined;

    var timer = try std.time.Timer.start();

    for (&producer_threads) |*t| {
        t.* = try std.Thread.spawn(.{}, struct {
            fn run(q: *Q, done: *std.atomic.Value(usize)) void {
                var tok = q.makeProducerToken() catch return;
                defer tok.deinit();
                for (0..ITEMS_PER_PRODUCER) |i| q.enqueue(&tok, @intCast(i)) catch return;
                _ = done.fetchAdd(1, .release);
            }
        }.run, .{ &queue, &producers_done });
    }

    const items_total = NUM_PRODUCERS * ITEMS_PER_PRODUCER;
    var total_dequeued = std.atomic.Value(usize).init(0);

    for (&consumer_threads) |*t| {
        t.* = try std.Thread.spawn(.{}, struct {
            fn run(q: *Q, dequeued: *std.atomic.Value(usize), done: *std.atomic.Value(usize)) void {
                var ctok = q.makeConsumerToken();
                var local: usize = 0;
                while (true) {
                    if (q.tryDequeue(&ctok)) |_| {
                        local += 1;
                    } else {
                        if (done.load(.acquire) >= NUM_PRODUCERS) break;
                        std.atomic.spinLoopHint();
                    }
                }
                while (q.tryDequeue(&ctok)) |_| local += 1;
                _ = dequeued.fetchAdd(local, .monotonic);
            }
        }.run, .{ &queue, &total_dequeued, &producers_done });
    }

    for (&producer_threads) |*t| t.join();
    for (&consumer_threads) |*t| t.join();

    const elapsed_ns = timer.read();
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    const ops_per_sec = @as(f64, @floatFromInt(items_total)) / (@as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0);

    try stdout.print("blocking queue:\n", .{});
    try stdout.print("  total:      {d}\n", .{items_total});
    try stdout.print("  dequeued:   {d}\n", .{total_dequeued.load(.monotonic)});
    try stdout.print("  elapsed:    {d:.2} ms\n", .{elapsed_ms});
    try stdout.print("  throughput: {d:.0} ops/sec\n", .{ops_per_sec});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const stdout = std.io.getStdOut().writer();

    try stdout.print("zig-concurrent-queue benchmark\n", .{});
    try stdout.print("  producers: {d}  consumers: {d}  items/producer: {d}\n\n", .{
        NUM_PRODUCERS, NUM_CONSUMERS, ITEMS_PER_PRODUCER,
    });

    try benchNonBlocking(allocator, stdout);
    try benchBlocking(allocator, stdout);
}
