# zig-concurrent-queue

A lock-free, multi-producer multi-consumer concurrent queue written in Zig.

Block-based storage, per-producer sub-queues, no locks anywhere.
Built with Zig's comptime generics and first-class atomics.

## Why

Most lock-free queue implementations available for Zig are either simple
single-producer/single-consumer ring buffers or wrappers around C libraries.
This is a proper multi-producer multi-consumer queue with per-producer sub-queues
to minimize contention. The `zig build asm` target makes it easy to inspect
the generated assembly and verify the codegen is tight.

## Status

Work in progress. Core enqueue/dequeue with explicit producer and consumer
tokens is functional. Block-index provides O(1) block lookup during dequeue.
Reference-counted lock-free free list for block recycling.

Still to do:

- [ ] Bulk enqueue / dequeue
- [ ] Blocking queue variant (`wait_dequeue`)
- [ ] Pre-allocation / capacity hints

## Quick start

Requires **Zig 0.15+**.

```bash
zig build test
zig build bench --
zig build asm   # => zig-out/asm/concurrent_queue.s
```

## Usage

```zig
const ConcurrentQueue = @import("concurrent-queue").ConcurrentQueue;

const Q = ConcurrentQueue(u64, .{});
var queue = Q.init(allocator);
defer queue.deinit();

var ptok = try queue.makeProducerToken();
defer ptok.deinit();
var ctok = queue.makeConsumerToken();

try queue.enqueue(&ptok, 42);

if (queue.tryDequeue(&ctok)) |value| {
    // got 42
}

// or without tokens (slightly slower, uses thread-local storage):
try queue.enqueueImplicit(99);
if (queue.tryDequeueAny()) |value| {
    _ = value;
}
```

## Configuration

```zig
const Q = ConcurrentQueue(MyStruct, .{
    .block_size = 64,
    .explicit_consumer_consumption_quota = 512,
});
```

See `src/concurrent_queue.zig` for all available knobs.

## License

BSD-2-Clause.
