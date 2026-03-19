const std = @import("std");
const ConcurrentQueue = @import("concurrent-queue").ConcurrentQueue;

const NP = 4;
const NC = 4;
const N = 1_000_000;

pub fn main() !void {
    const Q = ConcurrentQueue(u64, .{});
    var q = try Q.initWithCapacity(std.heap.c_allocator, 4096);
    defer q.deinit();

    var done = std.atomic.Value(usize).init(0);
    var total = std.atomic.Value(usize).init(0);

    var pts: [NP]std.Thread = undefined;
    var cts: [NC]std.Thread = undefined;

    var timer = try std.time.Timer.start();

    for (&pts) |*t| t.* = try std.Thread.spawn(.{}, struct {
        fn f(queue: *Q, d: *std.atomic.Value(usize)) void {
            var tok = queue.makeProducerToken() catch return;
            defer tok.deinit();
            for (0..N) |i| queue.enqueue(&tok, @intCast(i)) catch return;
            _ = d.fetchAdd(1, .release);
        }
    }.f, .{ &q, &done });

    for (&cts) |*t| t.* = try std.Thread.spawn(.{}, struct {
        fn f(queue: *Q, d: *std.atomic.Value(usize), tot: *std.atomic.Value(usize)) void {
            var ctok = queue.makeConsumerToken();
            var local: usize = 0;
            while (true) {
                if (queue.tryDequeue(&ctok)) |_| {
                    local += 1;
                } else if (d.load(.acquire) >= NP) break;
            }
            while (queue.tryDequeue(&ctok)) |_| local += 1;
            _ = tot.fetchAdd(local, .monotonic);
        }
    }.f, .{ &q, &done, &total });

    for (&pts) |*t| t.join();
    for (&cts) |*t| t.join();
    while (q.tryDequeueAny()) |_| _ = total.fetchAdd(1, .monotonic);

    const ns = timer.read();
    const items: f64 = NP * N;
    const ops = items / (@as(f64, @floatFromInt(ns)) / 1e9);
    const w = std.io.getStdOut().writer();
    try w.print("{d:.0}\n", .{ops});
}
