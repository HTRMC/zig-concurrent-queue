# zig-concurrent-queue

A lock-free, multi-producer multi-consumer concurrent queue written in Zig.

Block-based storage, per-producer sub-queues, no locks anywhere.
Built with Zig's comptime generics and first-class atomics.

## Features

- Single and bulk enqueue/dequeue
- Explicit producer and consumer tokens for best throughput
- Token-less path via thread-local implicit producers
- `tryEnqueue` for zero-allocation enqueue (only uses pre-allocated blocks)
- `sizeApprox()` for approximate element count
- Pre-allocation via `initWithCapacity`
- Producer-local block reuse (circular chain, avoids global free list in steady state)
- Reference-counted lock-free free list for block recycling
- Block-index for O(1) block lookup during dequeue
- `BlockingConcurrentQueue` with `waitDequeue` (spins then blocks via condition variable)

## Quick start

Requires **Zig 0.15+**.

```bash
zig build test
zig build bench --
zig build asm   # => zig-out/asm/concurrent_queue.s
```

## Usage

```zig
const queue = @import("concurrent-queue");
const ConcurrentQueue = queue.ConcurrentQueue;

const Q = ConcurrentQueue(u64, .{});
var q = Q.init(allocator);
// or pre-allocate: var q = try Q.initWithCapacity(allocator, 1024);
defer q.deinit();

var ptok = try q.makeProducerToken();
defer ptok.deinit();
var ctok = q.makeConsumerToken();

// single
try q.enqueue(&ptok, 42);
if (q.tryDequeue(&ctok)) |value| _ = value;

// bulk
var items = [_]u64{ 1, 2, 3 };
_ = try q.enqueueBulk(&ptok, &items);
var out: [3]u64 = undefined;
_ = q.tryDequeueBulk(&ctok, &out);

// zero-allocation (only succeeds if pre-allocated space exists)
_ = q.tryEnqueue(&ptok, 99);

// blocking variant
const BlockingQ = queue.BlockingConcurrentQueue(u64, .{});
var bq = BlockingQ.init(allocator);
defer bq.deinit();
// bq.waitDequeue(&ctok) blocks until an item is available
```

## Configuration

```zig
const Q = ConcurrentQueue(MyStruct, .{
    .block_size = 64,
    .explicit_consumer_consumption_quota = 512,
});
```

## License

BSD-2-Clause.
