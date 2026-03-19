const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const Atomic = std.atomic.Value;

pub const Traits = struct {
    block_size: comptime_int = 32,
    explicit_block_empty_counter_threshold: comptime_int = 32,
    explicit_initial_index_size: comptime_int = 32,
    explicit_consumer_consumption_quota: comptime_int = 256,
    max_subqueue_size: usize = 0,
};

pub fn ConcurrentQueue(comptime T: type, comptime traits: Traits) type {
    const BLOCK_SIZE: usize = traits.block_size;
    const BLOCK_MASK: usize = BLOCK_SIZE - 1;
    const INITIAL_INDEX_SIZE: usize = traits.explicit_initial_index_size;
    const CONSUMPTION_QUOTA: usize = traits.explicit_consumer_consumption_quota;

    comptime {
        assert(BLOCK_SIZE >= 2 and std.math.isPowerOfTwo(BLOCK_SIZE));
        assert(std.math.isPowerOfTwo(INITIAL_INDEX_SIZE));
    }

    return struct {
        const Self = @This();

        // ----- Block -----

        const Block = struct {
            data: [BLOCK_SIZE]ElementSlot = undefined,
            empty_flags: if (BLOCK_SIZE <= traits.explicit_block_empty_counter_threshold)
                [BLOCK_SIZE]Atomic(u32)
            else
                void = if (BLOCK_SIZE <= traits.explicit_block_empty_counter_threshold)
                initFlags()
            else {},
            elements_completely_dequeued: Atomic(u32) = Atomic(u32).init(0),
            next: Atomic(?*Block) = Atomic(?*Block).init(null),
            free_list_refs: Atomic(u32) = Atomic(u32).init(0),
            free_list_next: Atomic(?*Block) = Atomic(?*Block).init(null),
            dynamically_allocated: bool = false,

            const ElementSlot = struct {
                raw: [element_size]u8 align(element_align) = undefined,
                fn ptr(self: *ElementSlot) *T {
                    return @ptrCast(&self.raw);
                }
            };
            const element_size = @max(@sizeOf(T), 1);
            const element_align = @max(@alignOf(T), 1);

            fn initFlags() [BLOCK_SIZE]Atomic(u32) {
                var flags: [BLOCK_SIZE]Atomic(u32) = undefined;
                for (&flags) |*f| {
                    f.* = Atomic(u32).init(0);
                }
                return flags;
            }

            fn setEmpty(self: *Block, index: usize) void {
                if (BLOCK_SIZE <= traits.explicit_block_empty_counter_threshold) {
                    self.empty_flags[index & BLOCK_MASK].store(1, .release);
                } else {
                    _ = self.elements_completely_dequeued.fetchAdd(1, .release);
                }
            }

            fn isFullyEmpty(self: *Block) bool {
                if (BLOCK_SIZE <= traits.explicit_block_empty_counter_threshold) {
                    for (&self.empty_flags) |*f| {
                        if (f.load(.acquire) == 0) return false;
                    }
                    return true;
                } else {
                    return self.elements_completely_dequeued.load(.acquire) == BLOCK_SIZE;
                }
            }

            fn resetEmpty(self: *Block) void {
                if (BLOCK_SIZE <= traits.explicit_block_empty_counter_threshold) {
                    for (&self.empty_flags) |*f| {
                        f.store(0, .monotonic);
                    }
                }
                self.elements_completely_dequeued.store(0, .monotonic);
            }
        };

        // ----- Block Index -----
        // Circular buffer that maps tail indices to blocks. The producer appends
        // entries as it allocates blocks. Consumers read it with acquire ordering
        // to find the block for a given dequeue index in O(1).

        const BlockIndexEntry = struct {
            base: usize,
            block: ?*Block,
        };

        const BlockIndexHeader = struct {
            size: usize,
            front: Atomic(usize),
            entries: [*]BlockIndexEntry,
            prev: ?*BlockIndexHeader,
        };

        // ----- Producer -----

        const Producer = struct {
            tail_index: Atomic(usize) = Atomic(usize).init(0),
            head_index: Atomic(usize) = Atomic(usize).init(0),
            dequeue_optimistic_count: Atomic(usize) = Atomic(usize).init(0),
            dequeue_overcommit: Atomic(usize) = Atomic(usize).init(0),

            tail_block: ?*Block = null,

            block_index: Atomic(?*BlockIndexHeader) = Atomic(?*BlockIndexHeader).init(null),
            pr_block_index_front: usize = 0,
            pr_block_index_size: usize = 0,
            pr_block_index_slots_used: usize = 0,
            pr_block_index_entries: ?[*]BlockIndexEntry = null,
            pr_block_index_raw: ?*BlockIndexHeader = null,

            parent: *Self = undefined,
            next_producer: Atomic(?*Producer) = Atomic(?*Producer).init(null),
            active: Atomic(bool) = Atomic(bool).init(true),

            fn initBlockIndex(self: *Producer, allocator: Allocator) !void {
                const entries = try allocator.alloc(BlockIndexEntry, INITIAL_INDEX_SIZE);
                const header = try allocator.create(BlockIndexHeader);
                header.* = .{
                    .size = INITIAL_INDEX_SIZE,
                    .front = Atomic(usize).init(0),
                    .entries = entries.ptr,
                    .prev = null,
                };
                self.pr_block_index_front = 0;
                self.pr_block_index_size = INITIAL_INDEX_SIZE;
                self.pr_block_index_slots_used = 0;
                self.pr_block_index_entries = entries.ptr;
                self.pr_block_index_raw = header;
                self.block_index.store(header, .release);
            }

            fn growBlockIndex(self: *Producer, allocator: Allocator) !void {
                const new_size = self.pr_block_index_size * 2;
                const new_entries = try allocator.alloc(BlockIndexEntry, new_size);
                const new_header = try allocator.create(BlockIndexHeader);

                const old_size = self.pr_block_index_size;
                const old_entries = self.pr_block_index_entries.?;
                const slots_used = self.pr_block_index_slots_used;

                var j: usize = 0;
                var i_raw = self.pr_block_index_front -% slots_used;
                while (j < slots_used) : (j += 1) {
                    new_entries[j] = old_entries[i_raw & (old_size - 1)];
                    i_raw +%= 1;
                }

                const new_front = if (slots_used > 0) slots_used - 1 else 0;
                new_header.* = .{
                    .size = new_size,
                    .front = Atomic(usize).init(new_front),
                    .entries = new_entries.ptr,
                    .prev = self.pr_block_index_raw,
                };

                self.pr_block_index_front = slots_used;
                self.pr_block_index_size = new_size;
                self.pr_block_index_entries = new_entries.ptr;
                self.pr_block_index_raw = new_header;

                self.block_index.store(new_header, .release);
            }

            fn publishBlockIndexEntry(self: *Producer, base: usize, block: *Block) void {
                const entries = self.pr_block_index_entries.?;
                const front = self.pr_block_index_front;
                const size = self.pr_block_index_size;

                entries[front & (size - 1)] = .{ .base = base, .block = block };

                const header = self.block_index.load(.monotonic).?;
                header.front.store(front, .release);

                self.pr_block_index_front = front +% 1;
                self.pr_block_index_slots_used += 1;
            }

            fn dequeue(self: *Producer, parent: *Self) ?T {
                const tail = self.tail_index.load(.monotonic);
                const overcommit = self.dequeue_overcommit.load(.monotonic);
                const opt_count = self.dequeue_optimistic_count.load(.monotonic);

                if (!circularLessThan(opt_count -% overcommit, tail)) {
                    return null;
                }

                std.atomic.fence(.acquire);

                const my_dequeue_count = self.dequeue_optimistic_count.fetchAdd(1, .monotonic);

                const tail2 = self.tail_index.load(.acquire);

                if (!circularLessThan(my_dequeue_count -% overcommit, tail2)) {
                    _ = self.dequeue_overcommit.fetchAdd(1, .release);
                    return null;
                }

                const index = self.head_index.fetchAdd(1, .acq_rel);

                const local_bi = self.block_index.load(.acquire) orelse return null;
                const local_front = local_bi.front.load(.acquire);
                const front_entry = local_bi.entries[local_front & (local_bi.size - 1)];

                const block_base = index & ~@as(usize, BLOCK_MASK);
                const offset_signed = @as(isize, @intCast(block_base)) - @as(isize, @intCast(front_entry.base));
                const offset = @as(usize, @intCast(@divTrunc(offset_signed, @as(isize, BLOCK_SIZE))));

                const entry_index = (local_front +% offset) & (local_bi.size - 1);
                const block = local_bi.entries[entry_index].block orelse return null;

                const slot_idx = index & BLOCK_MASK;
                const item = block.data[slot_idx].ptr().*;

                block.setEmpty(slot_idx);

                if (block.isFullyEmpty()) {
                    const entry = &local_bi.entries[entry_index];
                    entry.block = null;
                    parent.freeListAdd(block);
                }

                return item;
            }

            fn deinit(self: *Producer, allocator: Allocator) void {
                const head_idx = self.head_index.load(.monotonic);
                const tail_idx = self.tail_index.load(.monotonic);

                if (head_idx != tail_idx) {
                    const bi = self.block_index.load(.monotonic);
                    if (bi) |header| {
                        var idx = head_idx;
                        while (idx != tail_idx) {
                            const block_base = idx & ~@as(usize, BLOCK_MASK);
                            const front_val = header.front.load(.monotonic);
                            const front_base = header.entries[front_val & (header.size - 1)].base;
                            const off_signed = @as(isize, @intCast(block_base)) - @as(isize, @intCast(front_base));
                            const off = @as(usize, @intCast(@divTrunc(off_signed, @as(isize, BLOCK_SIZE))));
                            const ei = (front_val +% off) & (header.size - 1);
                            if (header.entries[ei].block) |block| {
                                const slot_idx = idx & BLOCK_MASK;
                                block.data[slot_idx].ptr().* = undefined;
                            }
                            idx +%= 1;
                        }
                    }
                }

                var bi_header = self.pr_block_index_raw;
                while (bi_header) |h| {
                    const prev = h.prev;
                    const entries_slice = h.entries[0..h.size];
                    for (entries_slice) |entry| {
                        if (entry.block) |block| {
                            allocator.destroy(block);
                        }
                    }
                    allocator.free(entries_slice);
                    allocator.destroy(h);
                    bi_header = prev;
                }
            }
        };

        // ----- Tokens -----

        pub const ProducerToken = struct {
            producer: *Producer,

            pub fn deinit(self: *ProducerToken) void {
                self.producer.active.store(false, .release);
            }
        };

        pub const ConsumerToken = struct {
            initial_offset: usize,
            last_known_global_offset: usize,
            items_consumed: usize,
            current_producer: ?*Producer,
            desired_producer: ?*Producer,
        };

        // ----- Free List -----
        // Reference-counted lock-free free list. The high bit of free_list_refs
        // is SHOULD_BE_ON_FREELIST. The low 31 bits are a reference count for
        // threads currently traversing the node.

        const REFS_MASK: u32 = 0x7FFF_FFFF;
        const SHOULD_BE_ON_FREELIST: u32 = 0x8000_0000;

        fn freeListAdd(self: *Self, block: *Block) void {
            const prev = block.free_list_refs.fetchAdd(SHOULD_BE_ON_FREELIST, .acq_rel);
            if (prev & REFS_MASK == 0) {
                self.freeListAddKnowingRefcountZero(block);
            }
        }

        fn freeListAddKnowingRefcountZero(self: *Self, block: *Block) void {
            var head = self.free_list_head.load(.monotonic);
            while (true) {
                block.free_list_next.store(if (head) |h| h else null, .monotonic);
                block.free_list_refs.store(1, .release);

                if (self.free_list_head.cmpxchgWeak(head, block, .release, .monotonic)) |new_head| {
                    head = new_head;
                    const refs = block.free_list_refs.fetchAdd(SHOULD_BE_ON_FREELIST -% 1, .acq_rel);
                    if (refs == 1) {
                        continue;
                    }
                    return;
                } else {
                    return;
                }
            }
        }

        fn freeListTryGet(self: *Self) ?*Block {
            var head = self.free_list_head.load(.acquire);
            while (head) |h| {
                const refs = h.free_list_refs.load(.monotonic);
                if (refs & REFS_MASK == 0 or
                    h.free_list_refs.cmpxchgWeak(refs, refs + 1, .acquire, .monotonic) != null)
                {
                    head = self.free_list_head.load(.acquire);
                    continue;
                }

                const next = h.free_list_next.load(.monotonic);
                if (self.free_list_head.cmpxchgWeak(head, next, .acquire, .monotonic)) |new_head| {
                    head = new_head;
                    const prev_refs = h.free_list_refs.fetchSub(1, .acq_rel);
                    if (prev_refs == SHOULD_BE_ON_FREELIST + 1) {
                        self.freeListAddKnowingRefcountZero(h);
                    }
                    continue;
                } else {
                    _ = h.free_list_refs.fetchSub(2, .release);
                    return h;
                }
            }
            return null;
        }

        // ----- Queue State -----

        allocator: Allocator,
        producer_list: Atomic(?*Producer) = Atomic(?*Producer).init(null),
        producer_count: Atomic(usize) = Atomic(usize).init(0),
        free_list_head: Atomic(?*Block) = Atomic(?*Block).init(null),
        global_explicit_consumer_offset: Atomic(usize) = Atomic(usize).init(0),

        // ----- Public API -----

        pub fn init(allocator: Allocator) Self {
            return Self{ .allocator = allocator };
        }

        pub fn deinit(self: *Self) void {
            var prod = self.producer_list.load(.acquire);
            while (prod) |p| {
                const next = p.next_producer.load(.monotonic);
                p.deinit(self.allocator);
                self.allocator.destroy(p);
                prod = next;
            }

            var blk = self.free_list_head.load(.monotonic);
            while (blk) |b| {
                const next = b.free_list_next.load(.monotonic);
                self.allocator.destroy(b);
                blk = next;
            }
        }

        pub fn makeProducerToken(self: *Self) !ProducerToken {
            const producer = try self.allocator.create(Producer);
            producer.* = Producer{};
            producer.parent = self;
            try producer.initBlockIndex(self.allocator);

            while (true) {
                const head = self.producer_list.load(.acquire);
                producer.next_producer.store(head, .monotonic);
                if (self.producer_list.cmpxchgWeak(head, producer, .release, .monotonic) == null) {
                    break;
                }
            }
            _ = self.producer_count.fetchAdd(1, .release);

            return ProducerToken{ .producer = producer };
        }

        pub fn makeConsumerToken(self: *Self) ConsumerToken {
            _ = self;
            const offset = struct {
                var counter = Atomic(usize).init(0);
            };
            return ConsumerToken{
                .initial_offset = offset.counter.fetchAdd(1, .monotonic),
                .last_known_global_offset = 0,
                .items_consumed = 0,
                .current_producer = null,
                .desired_producer = null,
            };
        }

        pub fn enqueue(self: *Self, token: *ProducerToken, item: T) !void {
            const producer = token.producer;
            const tail = producer.tail_index.load(.monotonic);
            const slot_idx = tail & BLOCK_MASK;

            if (slot_idx == 0 or producer.tail_block == null) {
                if (producer.pr_block_index_slots_used >= producer.pr_block_index_size) {
                    try producer.growBlockIndex(self.allocator);
                }

                const block = self.requisitionBlock() catch |err| return err;
                block.resetEmpty();
                block.next.store(null, .monotonic);

                if (producer.tail_block) |tb| {
                    tb.next.store(block, .release);
                }
                producer.tail_block = block;

                producer.publishBlockIndexEntry(tail, block);
            }

            const block = producer.tail_block.?;
            block.data[slot_idx].ptr().* = item;

            producer.tail_index.store(tail +% 1, .release);
        }

        pub fn tryDequeue(self: *Self, token: *ConsumerToken) ?T {
            self.updateConsumerAfterRotation(token);

            if (token.current_producer) |prod| {
                if (prod.dequeue(self)) |item| {
                    token.items_consumed += 1;
                    if (token.items_consumed >= CONSUMPTION_QUOTA) {
                        _ = self.global_explicit_consumer_offset.fetchAdd(1, .monotonic);
                        token.items_consumed = 0;
                    }
                    return item;
                }
            }

            return self.tryDequeueFromAnyProducer(token);
        }

        pub fn tryDequeueAny(self: *Self) ?T {
            var prod = self.producer_list.load(.acquire);
            while (prod) |p| {
                if (p.dequeue(self)) |item| {
                    return item;
                }
                prod = p.next_producer.load(.acquire);
            }
            return null;
        }

        fn tryDequeueFromAnyProducer(self: *Self, token: *ConsumerToken) ?T {
            const start = token.current_producer orelse self.producer_list.load(.acquire);
            var prod = start;
            while (prod) |p| {
                if (p.dequeue(self)) |item| {
                    token.current_producer = p;
                    token.items_consumed = 1;
                    return item;
                }
                prod = p.next_producer.load(.acquire);
                if (prod == null) {
                    prod = self.producer_list.load(.acquire);
                }
                if (prod) |next| {
                    if (next == start orelse false) break;
                } else break;
            }
            return null;
        }

        fn updateConsumerAfterRotation(self: *Self, token: *ConsumerToken) void {
            const global = self.global_explicit_consumer_offset.load(.monotonic);

            if (token.desired_producer == null or token.last_known_global_offset != global) {
                const prod_count = self.producer_count.load(.monotonic);
                if (prod_count == 0) return;

                if (token.desired_producer == null) {
                    const target = prod_count - 1 - (token.initial_offset % prod_count);
                    var p = self.producer_list.load(.acquire);
                    var i: usize = 0;
                    while (p) |producer| {
                        if (i == target) {
                            token.desired_producer = producer;
                            break;
                        }
                        i += 1;
                        p = producer.next_producer.load(.acquire);
                    }
                } else {
                    const delta = @min(global -% token.last_known_global_offset, prod_count);
                    var p = token.desired_producer;
                    var i: usize = 0;
                    while (i < delta) : (i += 1) {
                        if (p) |producer| {
                            p = producer.next_producer.load(.acquire);
                        }
                        if (p == null) {
                            p = self.producer_list.load(.acquire);
                        }
                    }
                    token.desired_producer = p;
                }

                token.current_producer = token.desired_producer;
                token.items_consumed = 0;
                token.last_known_global_offset = global;
            }
        }

        fn requisitionBlock(self: *Self) !*Block {
            if (self.freeListTryGet()) |block| {
                block.resetEmpty();
                block.free_list_refs.store(0, .monotonic);
                block.free_list_next.store(null, .monotonic);
                return block;
            }

            const block = try self.allocator.create(Block);
            block.* = Block{};
            block.dynamically_allocated = true;
            return block;
        }

        threadlocal var tls_implicit_token: ?ProducerToken = null;
        threadlocal var tls_implicit_consumer: ?ConsumerToken = null;

        pub fn enqueueImplicit(self: *Self, item: T) !void {
            if (tls_implicit_token == null) {
                tls_implicit_token = try self.makeProducerToken();
            }
            try self.enqueue(&tls_implicit_token.?, item);
        }

        pub fn tryDequeueImplicit(self: *Self) ?T {
            if (tls_implicit_consumer == null) {
                tls_implicit_consumer = self.makeConsumerToken();
            }
            return self.tryDequeue(&tls_implicit_consumer.?);
        }

        fn circularLessThan(a: usize, b: usize) bool {
            const diff = a -% b;
            return diff > (@as(usize, 1) << (@bitSizeOf(usize) - 1));
        }
    };
}

const testing = std.testing;

test "single-threaded enqueue + dequeue" {
    const Q = ConcurrentQueue(u64, .{});
    var q = Q.init(testing.allocator);
    defer q.deinit();

    var ptok = try q.makeProducerToken();
    defer ptok.deinit();
    var ctok = q.makeConsumerToken();

    try q.enqueue(&ptok, 42);
    try q.enqueue(&ptok, 7);

    try testing.expectEqual(@as(?u64, 42), q.tryDequeue(&ctok));
    try testing.expectEqual(@as(?u64, 7), q.tryDequeue(&ctok));
    try testing.expectEqual(@as(?u64, null), q.tryDequeue(&ctok));
}

test "enqueue fills multiple blocks" {
    const Q = ConcurrentQueue(u32, .{ .block_size = 4 });
    var q = Q.init(testing.allocator);
    defer q.deinit();

    var ptok = try q.makeProducerToken();
    defer ptok.deinit();
    var ctok = q.makeConsumerToken();

    for (0..12) |i| {
        try q.enqueue(&ptok, @intCast(i));
    }
    for (0..12) |i| {
        try testing.expectEqual(@as(?u32, @intCast(i)), q.tryDequeue(&ctok));
    }
    try testing.expectEqual(@as(?u32, null), q.tryDequeue(&ctok));
}

test "many blocks" {
    const Q = ConcurrentQueue(u64, .{ .block_size = 4 });
    var q = Q.init(testing.allocator);
    defer q.deinit();

    var ptok = try q.makeProducerToken();
    defer ptok.deinit();
    var ctok = q.makeConsumerToken();

    const count = 256;
    for (0..count) |i| {
        try q.enqueue(&ptok, @intCast(i));
    }
    for (0..count) |i| {
        const val = q.tryDequeue(&ctok);
        try testing.expectEqual(@as(?u64, @intCast(i)), val);
    }
    try testing.expectEqual(@as(?u64, null), q.tryDequeue(&ctok));
}

test "tryDequeueAny without token" {
    const Q = ConcurrentQueue(u64, .{});
    var q = Q.init(testing.allocator);
    defer q.deinit();

    var ptok = try q.makeProducerToken();
    defer ptok.deinit();

    try q.enqueue(&ptok, 99);
    try testing.expectEqual(@as(?u64, 99), q.tryDequeueAny());
    try testing.expectEqual(@as(?u64, null), q.tryDequeueAny());
}

test "multi-threaded stress" {
    const Q = ConcurrentQueue(usize, .{});
    var q = Q.init(testing.allocator);
    defer q.deinit();

    const num_producers = 4;
    const num_consumers = 4;
    const items_per_producer = 10_000;
    var total_dequeued = Atomic(usize).init(0);
    var producers_done = Atomic(usize).init(0);

    var prod_threads: [num_producers]std.Thread = undefined;
    for (&prod_threads) |*t| {
        t.* = try std.Thread.spawn(.{}, struct {
            fn run(queue: *Q, done: *Atomic(usize)) void {
                var tok = queue.makeProducerToken() catch return;
                defer tok.deinit();
                for (0..items_per_producer) |i| {
                    queue.enqueue(&tok, i) catch return;
                }
                _ = done.fetchAdd(1, .release);
            }
        }.run, .{ &q, &producers_done });
    }

    var con_threads: [num_consumers]std.Thread = undefined;
    for (&con_threads) |*t| {
        t.* = try std.Thread.spawn(.{}, struct {
            fn run(queue: *Q, dequeued: *Atomic(usize), done: *Atomic(usize)) void {
                var local: usize = 0;
                while (done.load(.acquire) < num_producers or queue.tryDequeueAny() != null) {
                    if (queue.tryDequeueAny()) |_| {
                        local += 1;
                    } else {
                        std.atomic.spinLoopHint();
                    }
                }
                while (queue.tryDequeueAny()) |_| {
                    local += 1;
                }
                _ = dequeued.fetchAdd(local, .monotonic);
            }
        }.run, .{ &q, &total_dequeued, &producers_done });
    }

    for (&prod_threads) |*t| t.join();
    for (&con_threads) |*t| t.join();

    while (q.tryDequeueAny()) |_| {
        _ = total_dequeued.fetchAdd(1, .monotonic);
    }

    try testing.expectEqual(num_producers * items_per_producer, total_dequeued.load(.monotonic));
}

test "multi-producer single-consumer" {
    const Q = ConcurrentQueue(usize, .{ .block_size = 8 });
    var q = Q.init(testing.allocator);
    defer q.deinit();

    const num_producers = 8;
    const items_per_producer = 5_000;

    var prod_threads: [num_producers]std.Thread = undefined;
    for (&prod_threads) |*t| {
        t.* = try std.Thread.spawn(.{}, struct {
            fn run(queue: *Q) void {
                var tok = queue.makeProducerToken() catch return;
                defer tok.deinit();
                for (0..items_per_producer) |i| {
                    queue.enqueue(&tok, i) catch return;
                }
            }
        }.run, .{&q});
    }

    for (&prod_threads) |*t| t.join();

    var count: usize = 0;
    while (q.tryDequeueAny()) |_| {
        count += 1;
    }
    try testing.expectEqual(num_producers * items_per_producer, count);
}
