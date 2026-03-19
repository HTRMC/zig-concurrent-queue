const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

pub const Traits = struct {
    block_size: comptime_int = 32,
    explicit_block_empty_counter_threshold: comptime_int = 32,
    explicit_initial_index_size: comptime_int = 32,
    implicit_initial_index_size: comptime_int = 32,
    initial_implicit_producer_hash_size: comptime_int = 32,
    explicit_consumer_consumption_quota: comptime_int = 256,
    max_subqueue_size: usize = 0,
};

pub fn ConcurrentQueue(comptime T: type, comptime traits: Traits) type {
    const BLOCK_SIZE = traits.block_size;
    comptime {
        assert(BLOCK_SIZE >= 2 and std.math.isPowerOfTwo(BLOCK_SIZE));
    }

    return struct {
        const Self = @This();

        const Block = struct {
            data: [BLOCK_SIZE]ElementSlot = undefined,
            empty_flags: [BLOCK_SIZE]std.atomic.Value(u32) = blk: {
                var flags: [BLOCK_SIZE]std.atomic.Value(u32) = undefined;
                for (&flags) |*f| {
                    f.* = std.atomic.Value(u32).init(0);
                }
                break :blk flags;
            },
            empty_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
            next: std.atomic.Value(?*Block) = std.atomic.Value(?*Block).init(null),
            dynamically_allocated: bool = false,

            const ElementSlot = struct {
                raw: [element_size]u8 align(element_align) = undefined,

                fn ptr(self: *ElementSlot) *T {
                    return @ptrCast(&self.raw);
                }
            };
            const element_size = @max(@sizeOf(T), 1);
            const element_align = @max(@alignOf(T), 1);

            fn setEmpty(self: *Block, index: usize) void {
                if (BLOCK_SIZE <= traits.explicit_block_empty_counter_threshold) {
                    self.empty_flags[index].store(1, .release);
                } else {
                    _ = self.empty_count.fetchAdd(1, .release);
                }
            }

            fn isEmpty(self: *Block) bool {
                if (BLOCK_SIZE <= traits.explicit_block_empty_counter_threshold) {
                    for (&self.empty_flags) |*f| {
                        if (f.load(.acquire) == 0) return false;
                    }
                    return true;
                } else {
                    return self.empty_count.load(.acquire) == BLOCK_SIZE;
                }
            }

            fn resetEmpty(self: *Block) void {
                if (BLOCK_SIZE <= traits.explicit_block_empty_counter_threshold) {
                    for (&self.empty_flags) |*f| {
                        f.store(0, .monotonic);
                    }
                } else {
                    self.empty_count.store(0, .monotonic);
                }
            }
        };

        const Producer = struct {
            tail_index: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
            head_index: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
            dequeue_optimistic_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
            dequeue_overcommit: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
            tail_block: ?*Block = null,
            head_block: ?*Block = null,
            block_count: usize = 0,
            parent: *Self = undefined,
            next_producer: std.atomic.Value(?*Producer) = std.atomic.Value(?*Producer).init(null),
            active: std.atomic.Value(bool) = std.atomic.Value(bool).init(true),

            fn deinit(self: *Producer, allocator: Allocator) void {
                var block = self.head_block;
                if (block == null) return;

                const head_idx = self.head_index.load(.monotonic);
                const tail_idx = self.tail_index.load(.monotonic);

                if (head_idx != tail_idx) {
                    var idx = head_idx;
                    var b = block.?;
                    while (idx != tail_idx) {
                        const slot_idx = idx & (BLOCK_SIZE - 1);
                        const slot = &b.data[slot_idx];
                        slot.ptr().* = undefined;
                        idx +%= 1;
                        if (slot_idx == BLOCK_SIZE - 1) {
                            b = b.next.load(.monotonic) orelse break;
                        }
                    }
                }

                const first = block.?;
                var current = first;
                while (true) {
                    const next = current.next.load(.monotonic);
                    allocator.destroy(current);
                    if (next == null or next.? == first) break;
                    current = next.?;
                }
            }
        };

        pub const ProducerToken = struct {
            producer: *Producer,

            pub fn deinit(self: *ProducerToken) void {
                self.producer.active.store(false, .release);
            }
        };

        allocator: Allocator,
        producer_list: std.atomic.Value(?*Producer) = std.atomic.Value(?*Producer).init(null),
        producer_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        free_list: std.atomic.Value(?*Block) = std.atomic.Value(?*Block).init(null),

        pub fn init(allocator: Allocator) Self {
            return Self{
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Self) void {
            var prod = self.producer_list.load(.acquire);
            while (prod) |p| {
                const next = p.next_producer.load(.monotonic);
                p.deinit(self.allocator);
                self.allocator.destroy(p);
                prod = next;
            }

            var blk = self.free_list.load(.monotonic);
            while (blk) |b| {
                const next = b.next.load(.monotonic);
                self.allocator.destroy(b);
                blk = next;
            }
        }

        pub fn makeProducerToken(self: *Self) !ProducerToken {
            const producer = try self.allocator.create(Producer);
            producer.* = Producer{};
            producer.parent = self;

            while (true) {
                const head = self.producer_list.load(.acquire);
                producer.next_producer.store(head, .monotonic);
                if (self.producer_list.cmpxchgWeak(head, producer, .release, .monotonic)) |_| {
                    continue;
                } else {
                    break;
                }
            }
            _ = self.producer_count.fetchAdd(1, .release);

            return ProducerToken{ .producer = producer };
        }

        pub fn enqueue(self: *Self, token: *ProducerToken, item: T) !void {
            const producer = token.producer;
            const tail = producer.tail_index.load(.monotonic);
            const slot_idx = tail & (BLOCK_SIZE - 1);

            if (slot_idx == 0 or producer.tail_block == null) {
                const block = try self.requisitionBlock();
                block.resetEmpty();

                if (producer.tail_block) |tb| {
                    tb.next.store(block, .release);
                } else {
                    producer.head_block = block;
                }
                producer.tail_block = block;
                producer.block_count += 1;
            }

            const block = producer.tail_block.?;
            const slot = &block.data[slot_idx];
            slot.ptr().* = item;

            producer.tail_index.store(tail +% 1, .release);
        }

        pub fn enqueueImplicit(self: *Self, item: T) !void {
            var token = try self.getOrCreateImplicitToken();
            try self.enqueue(&token, item);
        }

        pub fn tryDequeue(self: *Self) ?T {
            var prod = self.producer_list.load(.acquire);
            const start = prod;
            while (prod) |p| {
                if (p.active.load(.acquire) or p.head_index.load(.monotonic) != p.tail_index.load(.monotonic)) {
                    if (self.tryDequeueFromProducer(p)) |item| {
                        return item;
                    }
                }
                prod = p.next_producer.load(.acquire);
                if (prod == null) prod = start;
                if (prod == start) break;
            }
            return null;
        }

        fn tryDequeueFromProducer(self: *Self, producer: *Producer) ?T {
            const tail = producer.tail_index.load(.acquire);
            var head = producer.head_index.load(.monotonic);

            if (head >= tail) return null;

            head = producer.head_index.fetchAdd(1, .acq_rel);
            if (head >= tail) {
                _ = producer.head_index.fetchSub(1, .monotonic);
                return null;
            }

            const slot_idx = head & (BLOCK_SIZE - 1);

            var block = producer.head_block orelse return null;
            const blocks_to_skip = (head / BLOCK_SIZE) -% (producer.head_index.load(.monotonic) -% 1) / BLOCK_SIZE;
            _ = blocks_to_skip;

            const item = block.data[slot_idx].ptr().*;

            block.setEmpty(slot_idx);

            if (slot_idx == BLOCK_SIZE - 1 and block.isEmpty()) {
                const next = block.next.load(.acquire);
                producer.head_block = next;
                self.recycleBlock(block);
            }

            return item;
        }

        fn requisitionBlock(self: *Self) !*Block {
            while (true) {
                const head = self.free_list.load(.acquire);
                if (head) |h| {
                    const next = h.next.load(.monotonic);
                    if (self.free_list.cmpxchgWeak(head, next, .release, .monotonic)) |_| {
                        continue;
                    } else {
                        h.next.store(null, .monotonic);
                        return h;
                    }
                } else break;
            }

            const block = try self.allocator.create(Block);
            block.* = Block{};
            block.dynamically_allocated = true;
            return block;
        }

        fn recycleBlock(self: *Self, block: *Block) void {
            block.resetEmpty();
            while (true) {
                const head = self.free_list.load(.acquire);
                block.next.store(head, .monotonic);
                if (self.free_list.cmpxchgWeak(head, block, .release, .monotonic)) |_| {
                    continue;
                } else break;
            }
        }

        threadlocal var tls_implicit_token: ?ProducerToken = null;

        fn getOrCreateImplicitToken(self: *Self) !ProducerToken {
            if (tls_implicit_token) |tok| return tok;
            const tok = try self.makeProducerToken();
            tls_implicit_token = tok;
            return tok;
        }
    };
}

const testing = std.testing;

test "single-threaded enqueue + dequeue" {
    var q = ConcurrentQueue(u64, .{}).init(testing.allocator);
    defer q.deinit();

    var token = try q.makeProducerToken();
    defer token.deinit();

    try q.enqueue(&token, 42);
    try q.enqueue(&token, 7);

    try testing.expectEqual(@as(?u64, 42), q.tryDequeue());
    try testing.expectEqual(@as(?u64, 7), q.tryDequeue());
    try testing.expectEqual(@as(?u64, null), q.tryDequeue());
}

test "enqueue fills multiple blocks" {
    var q = ConcurrentQueue(u32, .{ .block_size = 4 }).init(testing.allocator);
    defer q.deinit();

    var token = try q.makeProducerToken();
    defer token.deinit();

    for (0..12) |i| {
        try q.enqueue(&token, @intCast(i));
    }
    for (0..12) |i| {
        const v = q.tryDequeue();
        try testing.expectEqual(@as(?u32, @intCast(i)), v);
    }
    try testing.expectEqual(@as(?u32, null), q.tryDequeue());
}

test "multi-threaded smoke test" {
    const Q = ConcurrentQueue(usize, .{});
    var q = Q.init(testing.allocator);
    defer q.deinit();

    const num_threads = 4;
    const items_per_thread = 1000;
    var total_dequeued = std.atomic.Value(usize).init(0);

    var threads: [num_threads]std.Thread = undefined;
    for (&threads) |*t| {
        t.* = try std.Thread.spawn(.{}, struct {
            fn run(queue: *Q, dequeued: *std.atomic.Value(usize)) void {
                var tok = queue.makeProducerToken() catch return;
                defer tok.deinit();

                for (0..items_per_thread) |i| {
                    queue.enqueue(&tok, i) catch return;
                }
                var local: usize = 0;
                while (queue.tryDequeue()) |_| {
                    local += 1;
                }
                _ = dequeued.fetchAdd(local, .monotonic);
            }
        }.run, .{ &q, &total_dequeued });
    }

    for (&threads) |*t| t.join();

    while (q.tryDequeue()) |_| {
        _ = total_dequeued.fetchAdd(1, .monotonic);
    }

    try testing.expectEqual(num_threads * items_per_thread, total_dequeued.load(.monotonic));
}
