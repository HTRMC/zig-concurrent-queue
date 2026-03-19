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

Work in progress. The core single-element enqueue/dequeue path with explicit
producer tokens works. Still to do:

- [ ] Bulk enqueue / dequeue
- [ ] Implicit (token-less) producer path via thread-local storage
- [ ] Blocking queue variant (`wait_dequeue`)
- [ ] Block-index for O(1) block lookup during dequeue
- [ ] Pre-allocation / capacity hints
- [ ] Comprehensive stress tests

## Quick start

Requires **Zig 0.15+**.

```bash
# Run tests
zig build test

# Run benchmarks
zig build bench --

# Emit assembly for hot-path inspection
zig build asm
# => zig-out/asm/concurrent_queue.s
```

## Usage

```zig
const ConcurrentQueue = @import("concurrent-queue").ConcurrentQueue;

var queue = ConcurrentQueue(u64, .{}).init(allocator);
defer queue.deinit();

var token = try queue.makeProducerToken();
defer token.deinit();

try queue.enqueue(&token, 42);

if (queue.tryDequeue()) |value| {
    // got 42
}
```

## Configuration

Pass a custom `Traits` struct to tune block size, index sizes, and consumer
rotation quota:

```zig
const Q = ConcurrentQueue(MyStruct, .{
    .block_size = 64,
    .explicit_consumer_consumption_quota = 512,
});
```

See `src/concurrent_queue.zig` for all available knobs.

## License

BSD-2-Clause.
