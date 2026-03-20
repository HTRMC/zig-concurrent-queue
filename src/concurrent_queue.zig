const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const Atomic = std.atomic.Value;

fn compilerFence(comptime order: std.builtin.AtomicOrder) void {
    _ = order;
    asm volatile ("" ::: .{ .memory = true });
}

pub const Traits = struct {
    block_size: comptime_int = 32,
    explicit_block_empty_counter_threshold: comptime_int = 32,
    explicit_initial_index_size: comptime_int = 32,
    explicit_consumer_consumption_quota: comptime_int = 256,
    initial_implicit_producer_hash_size: comptime_int = 32,
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

        // ================================================================
        // Block
        // ================================================================

        pub const Block = struct {
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
                fn volatilePtr(self: *ElementSlot) *volatile T {
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

            inline fn setEmpty(self: *Block, index: usize) void {
                if (BLOCK_SIZE <= traits.explicit_block_empty_counter_threshold) {
                    self.empty_flags[index & BLOCK_MASK].store(1, .release);
                }
                _ = self.elements_completely_dequeued.fetchAdd(1, .release);
            }

            inline fn setManyEmpty(self: *Block, start: usize, count: usize) void {
                if (BLOCK_SIZE <= traits.explicit_block_empty_counter_threshold) {
                    var i: usize = 0;
                    while (i < count) : (i += 1) {
                        self.empty_flags[(start + i) & BLOCK_MASK].store(1, .release);
                    }
                }
                _ = self.elements_completely_dequeued.fetchAdd(@intCast(count), .release);
            }

            inline fn isFullyEmpty(self: *Block) bool {
                if (self.elements_completely_dequeued.load(.monotonic) == BLOCK_SIZE) {
                    compilerFence(.acquire);
                    return true;
                }
                return false;
            }

            inline fn resetEmpty(self: *Block) void {
                if (BLOCK_SIZE <= traits.explicit_block_empty_counter_threshold) {
                    const ptr: [*]u8 = @ptrCast(&self.empty_flags);
                    @memset(ptr[0 .. BLOCK_SIZE * @sizeOf(Atomic(u32))], 0);
                }
                self.elements_completely_dequeued.store(0, .monotonic);
            }

            inline fn setAllEmpty(self: *Block) void {
                self.elements_completely_dequeued.store(BLOCK_SIZE, .monotonic);
            }
        };

        // ================================================================
        // Block Index (explicit producer)
        // ================================================================

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

        // ================================================================
        // Block Index (implicit producer)
        // ================================================================

        const ImplicitBlockIndexEntry = struct {
            key: Atomic(usize),
            value: Atomic(?*Block),
        };

        const ImplicitBlockIndexHeader = struct {
            capacity: usize,
            tail: Atomic(usize),
            entries: [*]ImplicitBlockIndexEntry,
            index: [*]?*ImplicitBlockIndexEntry,
            prev: ?*ImplicitBlockIndexHeader,
        };

        // ================================================================
        // Producer base (common fields)
        // ================================================================

        pub const ProducerBase = struct {
            tail_index: Atomic(usize) = Atomic(usize).init(0),
            head_index: Atomic(usize) = Atomic(usize).init(0),
            dequeue_optimistic_count: Atomic(usize) = Atomic(usize).init(0),
            dequeue_overcommit: Atomic(usize) = Atomic(usize).init(0),
            next_producer: Atomic(?*ProducerBase) = Atomic(?*ProducerBase).init(null),
            active: Atomic(bool) = Atomic(bool).init(true),
            is_explicit: bool,

            fn sizeApprox(self: *const ProducerBase) usize {
                if (self.is_explicit) {
                    const ep: *const ExplicitProducer = @fieldParentPtr("base", @constCast(self));
                    const tail = ep.tail_index.load(.monotonic);
                    const head = ep.head_index.load(.monotonic);
                    return if (circularLessThan(head, tail)) tail -% head else 0;
                }
                const tail = self.tail_index.load(.monotonic);
                const head = self.head_index.load(.monotonic);
                return if (circularLessThan(head, tail)) tail -% head else 0;
            }
        };

        // ================================================================
        // Explicit Producer
        // ================================================================

        pub const ExplicitProducer = struct {
            // Hot fields first (same layout as C++ ProducerBase)
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
            base: ProducerBase = .{ .is_explicit = true },

            fn initBlockIndex(self: *ExplicitProducer, allocator: Allocator, pool_based_size: usize) !void {
                const size = @max(INITIAL_INDEX_SIZE, pool_based_size);
                const entries = try allocator.alloc(BlockIndexEntry, size);
                for (entries) |*e| e.* = .{ .base = 0, .block = null };
                const header = try allocator.create(BlockIndexHeader);
                header.* = .{
                    .size = size,
                    .front = Atomic(usize).init(0),
                    .entries = entries.ptr,
                    .prev = null,
                };
                self.pr_block_index_front = 0;
                self.pr_block_index_size = size;
                self.pr_block_index_slots_used = 0;
                self.pr_block_index_entries = entries.ptr;
                self.pr_block_index_raw = header;
                self.block_index.store(header, .release);
            }

            fn growBlockIndex(self: *ExplicitProducer, allocator: Allocator) !void {
                const new_size = self.pr_block_index_size * 2;
                const new_entries = try allocator.alloc(BlockIndexEntry, new_size);
                for (new_entries) |*e| e.* = .{ .base = 0, .block = null };
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

            inline fn publishBlockIndexEntry(self: *ExplicitProducer, base_val: usize, block: *Block) void {
                const entries = self.pr_block_index_entries.?;
                const front = self.pr_block_index_front;
                const size = self.pr_block_index_size;
                // Write fields separately (matching C++ two-store pattern)
                const entry = &entries[front & (size - 1)];
                entry.base = base_val;
                entry.block = block;
                const header = self.block_index.load(.monotonic).?;
                header.front.store(front, .release);
                self.pr_block_index_front = (front +% 1) & (size - 1);
            }

            inline fn signedBlockOffset(a: usize, b: usize) usize {
                const raw = a -% b;
                const signed: isize = @bitCast(raw);
                const shift = comptime std.math.log2_int(usize, BLOCK_SIZE);
                const off: isize = signed >> shift;
                return @bitCast(off);
            }

            inline fn lookupBlock(bi: *BlockIndexHeader, index: usize) ?*Block {
                const local_front = bi.front.load(.acquire);
                const front_entry = bi.entries[local_front & (bi.size - 1)];
                const block_base = index & ~@as(usize, BLOCK_MASK);
                const offset = signedBlockOffset(block_base, front_entry.base);
                const entry_index = (local_front +% offset) & (bi.size - 1);
                return bi.entries[entry_index].block;
            }

            inline fn enqueueOne(self: *ExplicitProducer, parent: *Self, item: T) !void {
                const tail = self.tail_index.load(.monotonic);
                const slot_idx = tail & BLOCK_MASK;

                if (slot_idx == 0 or self.tail_block == null) {
                    @branchHint(.unlikely);
                    try self.enqueueNewBlock(parent, tail, item);
                    return;
                }

                const tb = self.tail_block orelse unreachable;
                tb.data[slot_idx].ptr().* = item;
                self.tail_index.store(tail +% 1, .release);
            }

            noinline fn enqueueNewBlock(self: *ExplicitProducer, parent: *Self, tail: usize, item: T) !void {
                if (self.tail_block) |tb| {
                    const next_opt = tb.next.load(.monotonic);
                    if (next_opt) |next_block| {
                        if (next_block.isFullyEmpty()) {
                            self.tail_block = next_block;
                            next_block.resetEmpty();
                            self.publishBlockIndexEntry(tail, next_block);
                            next_block.data[0].ptr().* = item;
                            self.tail_index.store(tail +% 1, .release);
                            return;
                        }
                    }
                }

                if (self.pr_block_index_slots_used >= self.pr_block_index_size) {
                    try self.growBlockIndex(parent.allocator);
                }

                const block = try parent.requisitionBlock();
                block.resetEmpty();

                if (self.tail_block) |tb| {
                    block.next.store(tb.next.load(.monotonic), .monotonic);
                    tb.next.store(block, .release);
                } else {
                    block.next.store(block, .monotonic);
                }
                self.tail_block = block;
                self.pr_block_index_slots_used += 1;
                self.publishBlockIndexEntry(tail, block);

                block.data[0].ptr().* = item;
                self.tail_index.store(tail +% 1, .release);
            }

            fn tryEnqueueOne(self: *ExplicitProducer, parent: *Self, item: T) bool {
                const tail = self.tail_index.load(.monotonic);
                const slot_idx = tail & BLOCK_MASK;

                if (slot_idx == 0 or self.tail_block == null) {
                    if (self.tail_block) |tb| {
                        const next_opt = tb.next.load(.monotonic);
                        if (next_opt) |next_block| {
                            if (next_block.isFullyEmpty()) {
                                self.tail_block = next_block;
                                next_block.resetEmpty();
                                self.publishBlockIndexEntry(tail, next_block);
                                next_block.data[0].ptr().* = item;
                                self.tail_index.store(tail +% 1, .release);
                                return true;
                            }
                        }
                    }

                    if (self.pr_block_index_slots_used >= self.pr_block_index_size) return false;

                    const block = parent.tryRequisitionBlock() orelse return false;
                    block.resetEmpty();
                    if (self.tail_block) |tb| {
                        block.next.store(tb.next.load(.monotonic), .monotonic);
                        tb.next.store(block, .release);
                    } else {
                        block.next.store(block, .monotonic);
                    }
                    self.tail_block = block;
                    self.pr_block_index_slots_used += 1;
                    self.publishBlockIndexEntry(tail, block);
                }

                self.tail_block.?.data[slot_idx].ptr().* = item;
                self.tail_index.store(tail +% 1, .release);
                return true;
            }

            fn enqueueBulk(self: *ExplicitProducer, parent: *Self, items: []const T) !usize {
                if (items.len == 0) return 0;
                const count = items.len;
                const start_tail = self.tail_index.load(.monotonic);
                var current_tail = start_tail;
                const new_tail = start_tail +% count;

                const start_block_base = (start_tail -% 1) & ~@as(usize, BLOCK_MASK);
                const end_block_base = (start_tail +% count -% 1) & ~@as(usize, BLOCK_MASK);
                var blocks_needed: usize = 0;
                if (start_tail & BLOCK_MASK == 0 or self.tail_block == null) {
                    blocks_needed = (end_block_base -% start_block_base) / BLOCK_SIZE + 1;
                } else {
                    blocks_needed = (end_block_base -% (start_block_base +% BLOCK_SIZE)) / BLOCK_SIZE + 1;
                    if (end_block_base == start_block_base) blocks_needed = 0;
                }

                const orig_front = self.pr_block_index_front;
                const orig_slots_used = self.pr_block_index_slots_used;
                const orig_tail_block = self.tail_block;
                var first_allocated: ?*Block = null;

                // Phase 1: reuse empty blocks in chain
                if (blocks_needed > 0 and self.tail_block != null) {
                    var tb = self.tail_block.?;
                    while (blocks_needed > 0) {
                        const next_opt = tb.next.load(.monotonic);
                        if (next_opt) |next_block| {
                            if (first_allocated != null and next_block == first_allocated.?) break;
                            if (!next_block.isFullyEmpty()) break;
                            tb = next_block;
                            if (first_allocated == null) first_allocated = tb;
                            current_tail = (current_tail & ~@as(usize, BLOCK_MASK)) +% BLOCK_SIZE;
                            self.publishBlockIndexEntry(current_tail, tb);
                            blocks_needed -= 1;
                        } else break;
                    }
                    self.tail_block = tb;
                }

                // Phase 2: allocate new blocks
                while (blocks_needed > 0) : (blocks_needed -= 1) {
                    if (self.pr_block_index_slots_used >= self.pr_block_index_size) {
                        self.growBlockIndex(parent.allocator) catch {
                            self.pr_block_index_front = orig_front;
                            self.pr_block_index_slots_used = orig_slots_used;
                            self.tail_block = orig_tail_block;
                            return error.OutOfMemory;
                        };
                    }
                    const block = parent.requisitionBlock() catch {
                        self.pr_block_index_front = orig_front;
                        self.pr_block_index_slots_used = orig_slots_used;
                        self.tail_block = orig_tail_block;
                        return error.OutOfMemory;
                    };
                    block.setAllEmpty();
                    current_tail = (current_tail & ~@as(usize, BLOCK_MASK)) +% BLOCK_SIZE;
                    if (self.tail_block) |tb| {
                        block.next.store(tb.next.load(.monotonic), .monotonic);
                        tb.next.store(block, .release);
                    } else {
                        block.next.store(block, .monotonic);
                    }
                    self.tail_block = block;
                    if (first_allocated == null) first_allocated = block;
                    self.pr_block_index_slots_used += 1;
                    self.publishBlockIndexEntry(current_tail, block);
                }

                // Reset empty flags on all newly acquired blocks
                if (first_allocated) |fa| {
                    var b = fa;
                    while (true) {
                        b.resetEmpty();
                        if (b == self.tail_block.?) break;
                        b = b.next.load(.monotonic) orelse break;
                    }
                }

                // Publish block index
                if (self.pr_block_index_slots_used > orig_slots_used) {
                    const header = self.block_index.load(.monotonic).?;
                    header.front.store((self.pr_block_index_front -% 1) & (self.pr_block_index_size - 1), .release);
                }

                // Phase 3: construct elements across blocks
                self.tail_block = orig_tail_block;
                current_tail = start_tail;

                if (current_tail & BLOCK_MASK == 0 and first_allocated != null) {
                    self.tail_block = first_allocated;
                }

                var item_idx: usize = 0;
                while (item_idx < count) {
                    var stop = ((current_tail & ~@as(usize, BLOCK_MASK)) +% BLOCK_SIZE);
                    if (circularLessThan(new_tail, stop) or new_tail == stop) stop = new_tail;
                    const n = stop -% current_tail;

                    const tb = self.tail_block.?;
                    var i: usize = 0;
                    while (i < n) : (i += 1) {
                        tb.data[(current_tail +% i) & BLOCK_MASK].ptr().* = items[item_idx + i];
                    }

                    item_idx += n;
                    current_tail +%= n;

                    if (item_idx < count) {
                        self.tail_block = tb.next.load(.monotonic);
                    }
                }

                self.tail_index.store(new_tail, .release);
                return count;
            }

            inline fn dequeueOne(self: *ExplicitProducer, parent: *Self) ?T {
                _ = parent;
                const tail = self.tail_index.load(.monotonic);
                const overcommit = self.dequeue_overcommit.load(.monotonic);
                const opt_count = self.dequeue_optimistic_count.load(.monotonic);

                if (!circularLessThan(opt_count -% overcommit, tail)) {
                    @branchHint(.unlikely);
                    return null;
                }

                compilerFence(.acquire);

                const my_dequeue_count = self.dequeue_optimistic_count.fetchAdd(1, .monotonic);
                const tail2 = self.tail_index.load(.acquire);

                if (!circularLessThan(my_dequeue_count -% overcommit, tail2)) {
                    @branchHint(.unlikely);
                    _ = self.dequeue_overcommit.fetchAdd(1, .release);
                    return null;
                }

                const index = self.head_index.fetchAdd(1, .acq_rel);
                const local_bi = self.block_index.load(.acquire) orelse {
                    @branchHint(.cold);
                    return null;
                };
                const block = lookupBlock(local_bi, index) orelse {
                    @branchHint(.cold);
                    return null;
                };
                const slot_idx = index & BLOCK_MASK;
                const item = block.data[slot_idx].ptr().*;
                block.setEmpty(slot_idx);
                return item;
            }

            fn dequeueBulk(self: *ExplicitProducer, out: []T) usize {
                const max = out.len;
                if (max == 0) return 0;

                const tail = self.tail_index.load(.monotonic);
                const overcommit = self.dequeue_overcommit.load(.monotonic);
                const opt_count = self.dequeue_optimistic_count.load(.monotonic);
                var desired = tail -% (opt_count -% overcommit);

                if (!circularLessThan(opt_count -% overcommit, tail)) return 0;

                desired = @min(desired, max);
                compilerFence(.acquire);

                const my_dequeue_count = self.dequeue_optimistic_count.fetchAdd(desired, .monotonic);
                const tail2 = self.tail_index.load(.acquire);
                var actual = tail2 -% (my_dequeue_count -% overcommit);
                if (!circularLessThan(my_dequeue_count -% overcommit, tail2)) actual = 0;
                actual = @min(desired, actual);

                if (actual < desired) {
                    _ = self.dequeue_overcommit.fetchAdd(desired - actual, .release);
                }
                if (actual == 0) return 0;

                const first_index = self.head_index.fetchAdd(actual, .acq_rel);
                const local_bi = self.block_index.load(.acquire) orelse return 0;
                const local_front = local_bi.front.load(.acquire);
                const front_entry = local_bi.entries[local_front & (local_bi.size - 1)];
                const first_block_base = first_index & ~@as(usize, BLOCK_MASK);
                const off = signedBlockOffset(first_block_base, front_entry.base);
                var index_index = (local_front +% off) & (local_bi.size - 1);

                var idx: usize = 0;
                var current = first_index;
                while (idx < actual) {
                    var end = ((current & ~@as(usize, BLOCK_MASK)) +% BLOCK_SIZE);
                    if (first_index +% actual < end) end = first_index +% actual;
                    const block = local_bi.entries[index_index].block orelse break;
                    const first_in_block = current;
                    while (current != end) {
                        out[idx] = block.data[current & BLOCK_MASK].ptr().*;
                        idx += 1;
                        current +%= 1;
                    }
                    block.setManyEmpty(first_in_block, current -% first_in_block);
                    index_index = (index_index +% 1) & (local_bi.size - 1);
                }
                return actual;
            }

            fn deinit(self: *ExplicitProducer, allocator: Allocator) void {
                if (self.tail_block) |tail| {
                    var block = tail.next.load(.monotonic) orelse tail;
                    while (block != tail) {
                        const next = block.next.load(.monotonic) orelse break;
                        if (block.dynamically_allocated) allocator.destroy(block);
                        block = next;
                    }
                    if (tail.dynamically_allocated) allocator.destroy(tail);
                }
                var bi_header = self.pr_block_index_raw;
                while (bi_header) |h| {
                    const prev = h.prev;
                    allocator.free(h.entries[0..h.size]);
                    allocator.destroy(h);
                    bi_header = prev;
                }
            }
        };

        // ================================================================
        // Implicit Producer
        // ================================================================

        const ImplicitProducer = struct {
            base: ProducerBase,
            tail_block: ?*Block = null,
            block_index: Atomic(?*ImplicitBlockIndexHeader) = Atomic(?*ImplicitBlockIndexHeader).init(null),
            pr_block_index_tail: usize = 0,
            pr_block_index_size: usize = 0,
            pr_block_index_raw: ?*ImplicitBlockIndexHeader = null,
            parent: *Self = undefined,

            fn initBlockIndex(self: *ImplicitProducer, allocator: Allocator) !void {
                const cap = INITIAL_INDEX_SIZE;
                const entries = try allocator.alloc(ImplicitBlockIndexEntry, cap);
                const index_ptrs = try allocator.alloc(?*ImplicitBlockIndexEntry, cap);
                const header = try allocator.create(ImplicitBlockIndexHeader);
                for (entries) |*e| {
                    e.key = Atomic(usize).init(std.math.maxInt(usize));
                    e.value = Atomic(?*Block).init(null);
                }
                for (index_ptrs) |*p| p.* = null;
                header.* = .{
                    .capacity = cap,
                    .tail = Atomic(usize).init(0),
                    .entries = entries.ptr,
                    .index = index_ptrs.ptr,
                    .prev = null,
                };
                self.pr_block_index_tail = 0;
                self.pr_block_index_size = cap;
                self.pr_block_index_raw = header;
                self.block_index.store(header, .release);
            }

            fn insertBlockIndexEntry(self: *ImplicitProducer, allocator: Allocator, key: usize) !*ImplicitBlockIndexEntry {
                const header = self.block_index.load(.monotonic).?;
                const tail = self.pr_block_index_tail;

                if (tail >= header.capacity) {
                    try self.growImplicitBlockIndex(allocator);
                    return self.insertBlockIndexEntry(allocator, key);
                }

                var slot: usize = 0;
                while (slot < header.capacity) : (slot += 1) {
                    if (header.entries[slot].key.load(.monotonic) == std.math.maxInt(usize)) break;
                }
                if (slot == header.capacity) {
                    try self.growImplicitBlockIndex(allocator);
                    return self.insertBlockIndexEntry(allocator, key);
                }

                const entry = &header.entries[slot];
                entry.key.store(key, .monotonic);
                header.index[tail & (header.capacity - 1)] = entry;
                const new_header = self.block_index.load(.monotonic).?;
                new_header.tail.store(tail, .release);
                self.pr_block_index_tail = tail + 1;
                return entry;
            }

            fn growImplicitBlockIndex(self: *ImplicitProducer, allocator: Allocator) !void {
                const old_header = self.pr_block_index_raw.?;
                const new_cap = old_header.capacity * 2;
                const new_entries = try allocator.alloc(ImplicitBlockIndexEntry, new_cap);
                const new_index = try allocator.alloc(?*ImplicitBlockIndexEntry, new_cap);
                const new_header = try allocator.create(ImplicitBlockIndexHeader);
                for (new_entries) |*e| {
                    e.key = Atomic(usize).init(std.math.maxInt(usize));
                    e.value = Atomic(?*Block).init(null);
                }
                for (new_index) |*p| p.* = null;

                // Copy old index pointers, re-linking to new entries
                const old_tail = self.pr_block_index_tail;
                var i: usize = 0;
                while (i < old_tail and i < old_header.capacity) : (i += 1) {
                    const old_entry = old_header.index[i & (old_header.capacity - 1)];
                    if (old_entry) |oe| {
                        const k = oe.key.load(.monotonic);
                        const v = oe.value.load(.monotonic);
                        new_entries[i].key.store(k, .monotonic);
                        new_entries[i].value.store(v, .monotonic);
                        new_index[i & (new_cap - 1)] = &new_entries[i];
                    }
                }

                new_header.* = .{
                    .capacity = new_cap,
                    .tail = Atomic(usize).init(old_tail),
                    .entries = new_entries.ptr,
                    .index = new_index.ptr,
                    .prev = self.pr_block_index_raw,
                };
                self.pr_block_index_size = new_cap;
                self.pr_block_index_raw = new_header;
                self.block_index.store(new_header, .release);
            }

            fn getBlockForIndex(self: *ImplicitProducer, index: usize) ?*Block {
                const bi = self.block_index.load(.acquire) orelse return null;
                const tail = bi.tail.load(.acquire);
                const idx_ptr = bi.index[tail & (bi.capacity - 1)] orelse return null;
                const tail_base = idx_ptr.key.load(.monotonic);
                const block_base = index & ~@as(usize, BLOCK_MASK);
                const raw = block_base -% tail_base;
                const signed: isize = @bitCast(raw);
                const off: usize = @bitCast(@divTrunc(signed, @as(isize, BLOCK_SIZE)));
                const entry_idx = (tail +% off) & (bi.capacity - 1);
                const entry = bi.index[entry_idx] orelse return null;
                return entry.value.load(.monotonic);
            }

            fn enqueueOne(self: *ImplicitProducer, parent: *Self, item: T) !void {
                const tail = self.base.tail_index.load(.monotonic);

                if (tail & BLOCK_MASK == 0 or self.tail_block == null) {
                    const idx_entry = try self.insertBlockIndexEntry(parent.allocator, tail);
                    const block = parent.requisitionBlock() catch {
                        idx_entry.value.store(null, .monotonic);
                        return error.OutOfMemory;
                    };
                    block.resetEmpty();
                    idx_entry.value.store(block, .monotonic);
                    self.tail_block = block;
                }

                self.tail_block.?.data[tail & BLOCK_MASK].ptr().* = item;
                self.base.tail_index.store(tail +% 1, .release);
            }

            fn dequeueOne(self: *ImplicitProducer, parent: *Self) ?T {
                const tail = self.base.tail_index.load(.monotonic);
                const overcommit = self.base.dequeue_overcommit.load(.monotonic);
                const opt_count = self.base.dequeue_optimistic_count.load(.monotonic);

                if (!circularLessThan(opt_count -% overcommit, tail)) return null;

                compilerFence(.acquire);

                const my_dequeue_count = self.base.dequeue_optimistic_count.fetchAdd(1, .monotonic);
                const tail2 = self.base.tail_index.load(.acquire);

                if (!circularLessThan(my_dequeue_count -% overcommit, tail2)) {
                    _ = self.base.dequeue_overcommit.fetchAdd(1, .release);
                    return null;
                }

                const index = self.base.head_index.fetchAdd(1, .acq_rel);
                const block = self.getBlockForIndex(index) orelse return null;
                const slot_idx = index & BLOCK_MASK;
                const item = block.data[slot_idx].ptr().*;

                _ = block.elements_completely_dequeued.fetchAdd(1, .acq_rel);
                if (block.elements_completely_dequeued.load(.monotonic) == BLOCK_SIZE) {
                    parent.freeListAdd(block);
                }

                return item;
            }

            fn deinit(self: *ImplicitProducer, allocator: Allocator) void {
                var header = self.pr_block_index_raw;
                while (header) |h| {
                    const prev = h.prev;
                    const entries = h.entries[0..h.capacity];
                    for (entries) |*e| {
                        const block = e.value.load(.monotonic);
                        if (block) |b| {
                            if (b.dynamically_allocated) {
                                allocator.destroy(b);
                            }
                        }
                    }
                    allocator.free(entries);
                    allocator.free(h.index[0..h.capacity]);
                    allocator.destroy(h);
                    header = prev;
                }
            }
        };

        // ================================================================
        // Tokens
        // ================================================================

        pub const ProducerToken = struct {
            producer: *ProducerBase,
            explicit: *ExplicitProducer,

            pub fn deinit(self: *ProducerToken) void {
                self.producer.active.store(false, .release);
            }
        };

        pub const ConsumerToken = struct {
            initial_offset: usize,
            items_consumed: usize,
            current_producer: ?*ProducerBase,
            desired_producer: ?*ProducerBase,
            current_explicit: ?*ExplicitProducer = null,
        };

        // ================================================================
        // Free list
        // ================================================================

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
                    if (refs == 1) continue;
                    return;
                } else return;
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

        // ================================================================
        // Implicit producer hash table
        // ================================================================

        const INVALID_THREAD_ID: usize = 0;
        const REUSABLE_THREAD_ID: usize = 1;

        const ImplicitProducerKVP = struct {
            key: Atomic(usize),
            value: ?*ImplicitProducer,
        };

        const ImplicitProducerHash = struct {
            capacity: usize,
            entries: [*]ImplicitProducerKVP,
            prev: ?*ImplicitProducerHash,
        };

        fn hashThreadId(id: usize) usize {
            var h = id;
            h ^= h >> 33;
            h *%= 0xff51afd7ed558ccd;
            h ^= h >> 33;
            h *%= 0xc4ceb9fe1a85ec53;
            h ^= h >> 33;
            return h;
        }

        fn getOrAddImplicitProducer(self: *Self) !*ImplicitProducer {
            const id = std.Thread.getCurrentId();
            const thread_id: usize = @intCast(id);
            const hashed = hashThreadId(thread_id);

            var hash = self.implicit_producer_hash.load(.acquire);
            while (hash) |h| {
                var idx = hashed;
                while (true) {
                    idx &= h.capacity - 1;
                    const probed = h.entries[idx].key.load(.monotonic);
                    if (probed == thread_id) return h.entries[idx].value.?;
                    if (probed == INVALID_THREAD_ID) break;
                    idx += 1;
                }
                hash = h.prev;
            }

            // Not found, create
            const producer = try self.allocator.create(ImplicitProducer);
            producer.* = ImplicitProducer{ .base = .{ .is_explicit = false } };
            producer.parent = self;
            try producer.initBlockIndex(self.allocator);

            // Add to producer list
            while (true) {
                const head = self.producer_list.load(.acquire);
                producer.base.next_producer.store(head, .monotonic);
                if (self.producer_list.cmpxchgWeak(head, &producer.base, .release, .monotonic) == null) break;
            }
            _ = self.producer_count.fetchAdd(1, .release);

            // Insert into hash
            try self.implicitProducerHashInsert(thread_id, producer);

            return producer;
        }

        fn implicitProducerHashInsert(self: *Self, thread_id: usize, producer: *ImplicitProducer) !void {
            var main_hash = self.implicit_producer_hash.load(.acquire);
            if (main_hash == null) {
                try self.initImplicitProducerHash();
                main_hash = self.implicit_producer_hash.load(.acquire);
            }
            const h = main_hash.?;
            const hashed = hashThreadId(thread_id);

            // Check if we need to resize (> 50% full)
            const count = self.implicit_producer_hash_count.fetchAdd(1, .monotonic) + 1;
            if (count >= h.capacity / 2) {
                try self.resizeImplicitProducerHash();
            }

            const cur_hash = self.implicit_producer_hash.load(.acquire).?;
            var idx = hashed;
            while (true) {
                idx &= cur_hash.capacity - 1;
                const probed = cur_hash.entries[idx].key.load(.monotonic);
                if (probed == INVALID_THREAD_ID or probed == REUSABLE_THREAD_ID) {
                    if (cur_hash.entries[idx].key.cmpxchgWeak(probed, thread_id, .seq_cst, .monotonic) == null) {
                        cur_hash.entries[idx].value = producer;
                        return;
                    }
                }
                idx += 1;
            }
        }

        fn initImplicitProducerHash(self: *Self) !void {
            const cap = traits.initial_implicit_producer_hash_size;
            const entries = try self.allocator.alloc(ImplicitProducerKVP, cap);
            for (entries) |*e| {
                e.key = Atomic(usize).init(INVALID_THREAD_ID);
                e.value = null;
            }
            const hash = try self.allocator.create(ImplicitProducerHash);
            hash.* = .{
                .capacity = cap,
                .entries = entries.ptr,
                .prev = null,
            };
            self.implicit_producer_hash.store(hash, .release);
        }

        fn resizeImplicitProducerHash(self: *Self) !void {
            const old = self.implicit_producer_hash.load(.acquire) orelse return;
            const new_cap = old.capacity * 2;
            const new_entries = try self.allocator.alloc(ImplicitProducerKVP, new_cap);
            for (new_entries) |*e| {
                e.key = Atomic(usize).init(INVALID_THREAD_ID);
                e.value = null;
            }
            const new_hash = try self.allocator.create(ImplicitProducerHash);
            new_hash.* = .{
                .capacity = new_cap,
                .entries = new_entries.ptr,
                .prev = old,
            };
            self.implicit_producer_hash.store(new_hash, .release);
        }

        // ================================================================
        // Queue state
        // ================================================================

        const SLAB_SIZE = 64;

        const BlockSlab = struct {
            index: Atomic(usize) = Atomic(usize).init(0),
            blocks: [SLAB_SIZE]Block,
            next: ?*BlockSlab,
        };

        allocator: Allocator,
        producer_list: Atomic(?*ProducerBase) = Atomic(?*ProducerBase).init(null),
        producer_count: Atomic(usize) = Atomic(usize).init(0),
        free_list_head: Atomic(?*Block) = Atomic(?*Block).init(null),
        initial_block_pool: ?[]Block = null,
        initial_block_pool_index: Atomic(usize) = Atomic(usize).init(0),
        implicit_producer_hash: Atomic(?*ImplicitProducerHash) = Atomic(?*ImplicitProducerHash).init(null),
        implicit_producer_hash_count: Atomic(usize) = Atomic(usize).init(0),
        slab_list: Atomic(?*BlockSlab) = Atomic(?*BlockSlab).init(null),

        // ================================================================
        // Public API
        // ================================================================

        pub fn init(allocator: Allocator) Self {
            return Self{ .allocator = allocator };
        }

        pub fn initWithCapacity(allocator: Allocator, capacity: usize) !Self {
            var self = Self{ .allocator = allocator };
            const block_count = (capacity + BLOCK_SIZE - 1) / BLOCK_SIZE;
            if (block_count > 0) {
                const pool = try allocator.alloc(Block, block_count);
                for (pool) |*b| {
                    b.* = Block{};
                    b.dynamically_allocated = false;
                }
                self.initial_block_pool = pool;
            }
            return self;
        }

        pub fn deinit(self: *Self) void {
            var prod = self.producer_list.load(.acquire);
            while (prod) |p| {
                const next = p.next_producer.load(.monotonic);
                if (p.is_explicit) {
                    const ep: *ExplicitProducer = @fieldParentPtr("base", p);
                    ep.deinit(self.allocator);
                    self.allocator.destroy(ep);
                } else {
                    const ip: *ImplicitProducer = @fieldParentPtr("base", p);
                    ip.deinit(self.allocator);
                    self.allocator.destroy(ip);
                }
                prod = next;
            }

            var blk = self.free_list_head.load(.monotonic);
            while (blk) |b| {
                const next = b.free_list_next.load(.monotonic);
                if (b.dynamically_allocated) self.allocator.destroy(b);
                blk = next;
            }

            // Free implicit producer hash tables
            var hash = self.implicit_producer_hash.load(.monotonic);
            while (hash) |h| {
                const prev = h.prev;
                self.allocator.free(h.entries[0..h.capacity]);
                self.allocator.destroy(h);
                hash = prev;
            }

            if (self.initial_block_pool) |pool| {
                self.allocator.free(pool);
            }

            var slab = self.slab_list.load(.monotonic);
            while (slab) |s| {
                const next = s.next;
                self.allocator.destroy(s);
                slab = next;
            }
        }

        pub fn makeProducerToken(self: *Self) !ProducerToken {
            const producer = try self.allocator.create(ExplicitProducer);
            producer.* = ExplicitProducer{};
            producer.parent = self;

            const pool_index_size = if (self.initial_block_pool) |pool|
                std.math.ceilPowerOfTwo(usize, pool.len) catch INITIAL_INDEX_SIZE
            else
                INITIAL_INDEX_SIZE;
            try producer.initBlockIndex(self.allocator, pool_index_size >> 1);

            while (true) {
                const head = self.producer_list.load(.acquire);
                producer.base.next_producer.store(head, .monotonic);
                if (self.producer_list.cmpxchgWeak(head, &producer.base, .release, .monotonic) == null) break;
            }
            _ = self.producer_count.fetchAdd(1, .release);

            return ProducerToken{ .producer = &producer.base, .explicit = producer };
        }

        pub fn makeConsumerToken(self: *Self) ConsumerToken {
            _ = self;
            const offset = struct {
                var counter = Atomic(usize).init(0);
            };
            return ConsumerToken{
                .initial_offset = offset.counter.fetchAdd(1, .monotonic),
                .items_consumed = 0,
                .current_producer = null,
                .desired_producer = null,
            };
        }

        pub inline fn enqueue(self: *Self, token: *ProducerToken, item: T) !void {
            return token.explicit.enqueueOne(self, item);
        }

        pub inline fn tryEnqueue(self: *Self, token: *ProducerToken, item: T) bool {
            return token.explicit.tryEnqueueOne(self, item);
        }

        pub fn enqueueBulk(self: *Self, token: *ProducerToken, items: []const T) !usize {
            return token.explicit.enqueueBulk(self, items);
        }

        pub inline fn tryDequeue(self: *Self, token: *ConsumerToken) ?T {
            if (token.current_explicit) |ep| {
                if (ep.dequeueOne(self)) |item| {
                    token.items_consumed += 1;
                    if (token.items_consumed >= CONSUMPTION_QUOTA) {
                        @branchHint(.unlikely);
                        self.rotateConsumer(token);
                    }
                    return item;
                }
            }
            return self.tryDequeueColdPath(token);
        }

        noinline fn tryDequeueColdPath(self: *Self, token: *ConsumerToken) ?T {
            if (token.current_explicit == null) {
                if (token.desired_producer == null) {
                    self.initConsumerProducer(token);
                }
                if (token.current_producer) |prod| {
                    if (prod.is_explicit) {
                        const ep: *ExplicitProducer = @fieldParentPtr("base", prod);
                        token.current_explicit = ep;
                        if (ep.dequeueOne(self)) |item| {
                            token.items_consumed = 1;
                            return item;
                        }
                    }
                }
            }
            return self.tryDequeueFromAnyProducer(token);
        }

        pub fn tryDequeueBulk(self: *Self, token: *ConsumerToken, out: []T) usize {
            var total: usize = 0;
            while (total < out.len) {
                if (self.tryDequeue(token)) |item| {
                    out[total] = item;
                    total += 1;
                } else break;
            }
            return total;
        }

        pub fn tryDequeueAny(self: *Self) ?T {
            var prod = self.producer_list.load(.acquire);
            while (prod) |p| {
                const item = self.dequeueFromProducer(p);
                if (item) |v| return v;
                prod = p.next_producer.load(.acquire);
            }
            return null;
        }

        pub fn sizeApprox(self: *const Self) usize {
            var size: usize = 0;
            var prod = self.producer_list.load(.acquire);
            while (prod) |p| {
                size += p.sizeApprox();
                prod = p.next_producer.load(.acquire);
            }
            return size;
        }

        pub fn enqueueImplicit(self: *Self, item: T) !void {
            const producer = try self.getOrAddImplicitProducer();
            return producer.enqueueOne(self, item);
        }

        pub fn tryDequeueImplicit(self: *Self) ?T {
            return self.tryDequeueAny();
        }

        // ================================================================
        // Internal
        // ================================================================

        fn dequeueFromProducer(self: *Self, prod: *ProducerBase) ?T {
            if (prod.is_explicit) {
                const ep: *ExplicitProducer = @fieldParentPtr("base", prod);
                return ep.dequeueOne(self);
            } else {
                const ip: *ImplicitProducer = @fieldParentPtr("base", prod);
                return ip.dequeueOne(self);
            }
        }

        fn tryDequeueFromAnyProducer(self: *Self, token: *ConsumerToken) ?T {
            const start = token.current_producer orelse self.producer_list.load(.acquire);
            var prod = start;
            while (prod) |p| {
                const item = self.dequeueFromProducer(p);
                if (item) |v| {
                    token.current_producer = p;
                    token.current_explicit = if (p.is_explicit)
                        @as(*ExplicitProducer, @fieldParentPtr("base", p))
                    else
                        null;
                    token.items_consumed = 1;
                    return v;
                }
                prod = p.next_producer.load(.acquire);
                if (prod == null) prod = self.producer_list.load(.acquire);
                if (prod) |next| {
                    if (next == start) break;
                } else break;
            }
            return null;
        }

        /// Per-consumer local rotation: advance to the next producer in the
        /// list.  No shared atomic — each consumer rotates independently,
        /// avoiding the cache-line bouncing of a global offset counter.
        noinline fn rotateConsumer(_: *Self, token: *ConsumerToken) void {
            token.items_consumed = 0;
            if (token.current_explicit) |ep| {
                const next = ep.base.next_producer.load(.acquire);
                // Wrap handled lazily: if next is null we keep desired as-is
                // and let tryDequeueFromAnyProducer pick up from the head.
                if (next) |n| {
                    token.desired_producer = n;
                    token.current_producer = n;
                    token.current_explicit = if (n.is_explicit)
                        @as(*ExplicitProducer, @fieldParentPtr("base", n))
                    else
                        null;
                    return;
                }
            }
            // Wrap around or non-explicit: clear cached producer so the cold
            // path re-acquires from the head of the producer list.
            token.current_explicit = null;
            token.current_producer = null;
            token.desired_producer = null;
        }

        /// One-time assignment: spread consumers across producers using
        /// their unique initial_offset.
        fn initConsumerProducer(self: *Self, token: *ConsumerToken) void {
            const prod_count = self.producer_count.load(.monotonic);
            if (prod_count == 0) return;
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
            token.current_producer = token.desired_producer;
        }

        noinline fn requisitionBlock(self: *Self) !*Block {
            if (self.tryBlockFromPool()) |block| return block;
            if (self.freeListTryGet()) |block| {
                block.resetEmpty();
                block.free_list_refs.store(0, .monotonic);
                block.free_list_next.store(null, .monotonic);
                return block;
            }
            return self.allocateBlockFromSlab();
        }

        fn tryRequisitionBlock(self: *Self) ?*Block {
            if (self.tryBlockFromPool()) |block| return block;
            if (self.freeListTryGet()) |block| {
                block.resetEmpty();
                block.free_list_refs.store(0, .monotonic);
                block.free_list_next.store(null, .monotonic);
                return block;
            }
            return null;
        }

        inline fn tryBlockFromPool(self: *Self) ?*Block {
            const pool = self.initial_block_pool orelse return null;
            const idx = self.initial_block_pool_index.fetchAdd(1, .monotonic);
            if (idx >= pool.len) return null;
            pool[idx].resetEmpty();
            pool[idx].free_list_refs.store(0, .monotonic);
            pool[idx].free_list_next.store(null, .monotonic);
            pool[idx].next.store(null, .monotonic);
            return &pool[idx];
        }

        fn allocateBlockFromSlab(self: *Self) !*Block {
            while (true) {
                const slab = self.slab_list.load(.acquire);

                if (slab) |s| {
                    // Try to claim a slot from the current slab
                    const idx = s.index.fetchAdd(1, .monotonic);
                    if (idx < SLAB_SIZE) {
                        const block = &s.blocks[idx];
                        block.* = Block{};
                        return block;
                    }
                    // Slab full — fall through to create a new one
                }

                // Allocate a new slab — reserve block 0 for ourselves
                const new_slab = try self.allocator.create(BlockSlab);
                new_slab.* = .{ .blocks = undefined, .next = slab };
                new_slab.index = Atomic(usize).init(1); // block 0 is ours

                // CAS to install as head. If we lose, free ours and retry.
                if (self.slab_list.cmpxchgWeak(slab, new_slab, .release, .acquire)) |_| {
                    self.allocator.destroy(new_slab);
                    continue;
                }

                const block = &new_slab.blocks[0];
                block.* = Block{};
                return block;
            }
        }

        inline fn circularLessThan(a: usize, b: usize) bool {
            const diff: isize = @bitCast(a -% b);
            return diff < 0;
        }
    };
}

// ====================================================================
// Lightweight Semaphore (for BlockingConcurrentQueue)
// ====================================================================

pub const LightweightSemaphore = struct {
    count: Atomic(isize) = Atomic(isize).init(0),
    mutex: std.Thread.Mutex = .{},
    cond: std.Thread.Condition = .{},
    waiters: usize = 0,

    pub fn signal(self: *LightweightSemaphore) void {
        self.signalN(1);
    }

    pub fn signalN(self: *LightweightSemaphore, n: usize) void {
        const old = self.count.fetchAdd(@intCast(n), .release);
        if (old < 0) {
            self.mutex.lock();
            defer self.mutex.unlock();
            const to_wake = @min(n, @as(usize, @intCast(-old)));
            for (0..to_wake) |_| self.cond.signal();
        }
    }

    pub fn tryWait(self: *LightweightSemaphore) bool {
        var c = self.count.load(.monotonic);
        while (c > 0) {
            if (self.count.cmpxchgWeak(c, c - 1, .acquire, .monotonic)) |v| {
                c = v;
            } else return true;
        }
        return false;
    }

    pub fn tryWaitMany(self: *LightweightSemaphore, max: usize) usize {
        var c = self.count.load(.monotonic);
        while (c > 0) {
            const to_take: isize = @intCast(@min(@as(usize, @intCast(c)), max));
            if (self.count.cmpxchgWeak(c, c - to_take, .acquire, .monotonic)) |v| {
                c = v;
            } else return @intCast(to_take);
        }
        return 0;
    }

    pub fn wait(self: *LightweightSemaphore) void {
        for (0..10000) |_| {
            if (self.tryWait()) return;
            std.atomic.spinLoopHint();
        }
        self.mutex.lock();
        defer self.mutex.unlock();
        self.waiters += 1;
        while (!self.tryWait()) self.cond.wait(&self.mutex);
        self.waiters -= 1;
    }

    pub fn waitMany(self: *LightweightSemaphore, max: usize) usize {
        assert(max > 0);
        const n = self.tryWaitMany(max);
        if (n > 0) return n;
        self.wait();
        return 1 + self.tryWaitMany(max - 1);
    }

    pub fn availableApprox(self: *const LightweightSemaphore) usize {
        const c = self.count.load(.monotonic);
        return if (c > 0) @intCast(c) else 0;
    }
};

// ====================================================================
// Blocking Concurrent Queue
// ====================================================================

pub fn BlockingConcurrentQueue(comptime T: type, comptime traits: Traits) type {
    const Inner = ConcurrentQueue(T, traits);

    return struct {
        const Self = @This();

        inner: Inner,
        sema: LightweightSemaphore = .{},

        pub fn init(allocator: Allocator) Self {
            return Self{ .inner = Inner.init(allocator) };
        }

        pub fn initWithCapacity(allocator: Allocator, capacity: usize) !Self {
            return Self{ .inner = try Inner.initWithCapacity(allocator, capacity) };
        }

        pub fn deinit(self: *Self) void {
            self.inner.deinit();
        }

        pub fn makeProducerToken(self: *Self) !Inner.ProducerToken {
            return self.inner.makeProducerToken();
        }

        pub fn makeConsumerToken(self: *Self) Inner.ConsumerToken {
            return self.inner.makeConsumerToken();
        }

        pub fn enqueue(self: *Self, token: *Inner.ProducerToken, item: T) !void {
            try self.inner.enqueue(token, item);
            self.sema.signal();
        }

        pub fn tryEnqueue(self: *Self, token: *Inner.ProducerToken, item: T) bool {
            if (self.inner.tryEnqueue(token, item)) {
                self.sema.signal();
                return true;
            }
            return false;
        }

        pub fn enqueueBulk(self: *Self, token: *Inner.ProducerToken, items: []const T) !usize {
            const n = try self.inner.enqueueBulk(token, items);
            if (n > 0) self.sema.signalN(n);
            return n;
        }

        pub fn tryDequeue(self: *Self, token: *Inner.ConsumerToken) ?T {
            if (self.sema.tryWait()) {
                while (true) {
                    if (self.inner.tryDequeue(token)) |item| return item;
                }
            }
            return null;
        }

        pub fn waitDequeue(self: *Self, token: *Inner.ConsumerToken) T {
            self.sema.wait();
            while (true) {
                if (self.inner.tryDequeue(token)) |item| return item;
            }
        }

        pub fn tryDequeueBulk(self: *Self, token: *Inner.ConsumerToken, out: []T) usize {
            const n = self.sema.tryWaitMany(out.len);
            if (n == 0) return 0;
            var total: usize = 0;
            while (total < n) {
                total += self.inner.tryDequeueBulk(token, out[total..n]);
            }
            return total;
        }

        pub fn waitDequeueBulk(self: *Self, token: *Inner.ConsumerToken, out: []T) usize {
            const n = self.sema.waitMany(out.len);
            var total: usize = 0;
            while (total < n) {
                total += self.inner.tryDequeueBulk(token, out[total..n]);
            }
            return total;
        }

        pub fn sizeApprox(self: *const Self) usize {
            return self.sema.availableApprox();
        }
    };
}

// ====================================================================
// Tests
// ====================================================================

const testing = std.testing;
const Atomic_usize = Atomic(usize);

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

    for (0..12) |i| try q.enqueue(&ptok, @intCast(i));
    for (0..12) |i| try testing.expectEqual(@as(?u32, @intCast(i)), q.tryDequeue(&ctok));
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
    for (0..count) |i| try q.enqueue(&ptok, @intCast(i));
    for (0..count) |i| try testing.expectEqual(@as(?u64, @intCast(i)), q.tryDequeue(&ctok));
    try testing.expectEqual(@as(?u64, null), q.tryDequeue(&ctok));
}

test "try_enqueue" {
    const Q = ConcurrentQueue(u64, .{});
    var q = Q.init(testing.allocator);
    defer q.deinit();

    var ptok = try q.makeProducerToken();
    defer ptok.deinit();

    // try_enqueue fails with no pre-allocated blocks
    // (no initial pool, no free list blocks)
    // Actually it will fail only if there's truly no space, but
    // since we haven't pre-allocated, the first call goes to the pool/freelist
    // which are empty, so it returns false.
    try testing.expect(!q.tryEnqueue(&ptok, 42));
}

test "try_enqueue with capacity" {
    const Q = ConcurrentQueue(u64, .{});
    var q = try Q.initWithCapacity(testing.allocator, 64);
    defer q.deinit();

    var ptok = try q.makeProducerToken();
    defer ptok.deinit();
    var ctok = q.makeConsumerToken();

    try testing.expect(q.tryEnqueue(&ptok, 42));
    try testing.expectEqual(@as(?u64, 42), q.tryDequeue(&ctok));
}

test "size_approx" {
    const Q = ConcurrentQueue(u64, .{});
    var q = Q.init(testing.allocator);
    defer q.deinit();

    var ptok = try q.makeProducerToken();
    defer ptok.deinit();
    var ctok = q.makeConsumerToken();

    try testing.expectEqual(@as(usize, 0), q.sizeApprox());
    try q.enqueue(&ptok, 1);
    try q.enqueue(&ptok, 2);
    try q.enqueue(&ptok, 3);
    try testing.expectEqual(@as(usize, 3), q.sizeApprox());
    _ = q.tryDequeue(&ctok);
    try testing.expectEqual(@as(usize, 2), q.sizeApprox());
}

test "bulk enqueue + dequeue" {
    const Q = ConcurrentQueue(u32, .{ .block_size = 4 });
    var q = Q.init(testing.allocator);
    defer q.deinit();

    var ptok = try q.makeProducerToken();
    defer ptok.deinit();
    var ctok = q.makeConsumerToken();

    var items: [20]u32 = undefined;
    for (&items, 0..) |*v, i| v.* = @intCast(i);

    for (&items) |item| try q.enqueue(&ptok, item);
    try testing.expectEqual(@as(usize, 20), q.sizeApprox());

    var out: [20]u32 = undefined;
    const dequeued = q.tryDequeueBulk(&ctok, &out);
    try testing.expectEqual(@as(usize, 20), dequeued);
    for (0..20) |i| try testing.expectEqual(@as(u32, @intCast(i)), out[i]);
}

test "pre-allocation" {
    const Q = ConcurrentQueue(u64, .{});
    var q = try Q.initWithCapacity(testing.allocator, 128);
    defer q.deinit();

    var ptok = try q.makeProducerToken();
    defer ptok.deinit();
    var ctok = q.makeConsumerToken();

    for (0..100) |i| try q.enqueue(&ptok, @intCast(i));
    for (0..100) |i| try testing.expectEqual(@as(?u64, @intCast(i)), q.tryDequeue(&ctok));
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
    var total_dequeued = Atomic_usize.init(0);
    var producers_done = Atomic_usize.init(0);

    var prod_threads: [num_producers]std.Thread = undefined;
    for (&prod_threads) |*t| {
        t.* = try std.Thread.spawn(.{}, struct {
            fn run(queue: *Q, done: *Atomic_usize) void {
                var tok = queue.makeProducerToken() catch return;
                defer tok.deinit();
                for (0..items_per_producer) |i| queue.enqueue(&tok, i) catch return;
                _ = done.fetchAdd(1, .release);
            }
        }.run, .{ &q, &producers_done });
    }

    var con_threads: [num_consumers]std.Thread = undefined;
    for (&con_threads) |*t| {
        t.* = try std.Thread.spawn(.{}, struct {
            fn run(queue: *Q, dequeued: *Atomic_usize, done: *Atomic_usize) void {
                var local: usize = 0;
                while (true) {
                    if (queue.tryDequeueAny()) |_| {
                        local += 1;
                    } else {
                        if (done.load(.acquire) >= num_producers) break;
                        std.atomic.spinLoopHint();
                    }
                }
                while (queue.tryDequeueAny()) |_| local += 1;
                _ = dequeued.fetchAdd(local, .monotonic);
            }
        }.run, .{ &q, &total_dequeued, &producers_done });
    }

    for (&prod_threads) |*t| t.join();
    for (&con_threads) |*t| t.join();
    while (q.tryDequeueAny()) |_| _ = total_dequeued.fetchAdd(1, .monotonic);

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
                for (0..items_per_producer) |i| queue.enqueue(&tok, i) catch return;
            }
        }.run, .{&q});
    }

    for (&prod_threads) |*t| t.join();

    var count: usize = 0;
    while (q.tryDequeueAny()) |_| count += 1;
    try testing.expectEqual(num_producers * items_per_producer, count);
}

test "blocking queue basic" {
    const Q = BlockingConcurrentQueue(u64, .{});
    var q = Q.init(testing.allocator);
    defer q.deinit();

    var ptok = try q.makeProducerToken();
    defer ptok.deinit();
    var ctok = q.makeConsumerToken();

    try q.enqueue(&ptok, 42);
    try q.enqueue(&ptok, 7);

    try testing.expectEqual(@as(u64, 42), q.waitDequeue(&ctok));
    try testing.expectEqual(@as(u64, 7), q.waitDequeue(&ctok));
    try testing.expectEqual(@as(?u64, null), q.tryDequeue(&ctok));
}

test "blocking queue multi-threaded" {
    const Q = BlockingConcurrentQueue(usize, .{});
    var q = Q.init(testing.allocator);
    defer q.deinit();

    const num_items = 10_000;
    var total = Atomic_usize.init(0);

    const producer = try std.Thread.spawn(.{}, struct {
        fn run(queue: *Q) void {
            var tok = queue.makeProducerToken() catch return;
            defer tok.deinit();
            for (0..num_items) |i| queue.enqueue(&tok, i) catch return;
        }
    }.run, .{&q});

    const consumer = try std.Thread.spawn(.{}, struct {
        fn run(queue: *Q, t: *Atomic_usize) void {
            var ctok = queue.makeConsumerToken();
            var local: usize = 0;
            while (local < num_items) {
                _ = queue.waitDequeue(&ctok);
                local += 1;
            }
            _ = t.fetchAdd(local, .monotonic);
        }
    }.run, .{ &q, &total });

    producer.join();
    consumer.join();

    try testing.expectEqual(num_items, total.load(.monotonic));
}
