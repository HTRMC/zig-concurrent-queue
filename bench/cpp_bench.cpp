#include "concurrentqueue.h"
#include <thread>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <vector>

static constexpr int NUM_PRODUCERS = 4;
static constexpr int NUM_CONSUMERS = 4;
static constexpr int ITEMS_PER_PRODUCER = 1'000'000;

int main() {
    moodycamel::ConcurrentQueue<uint64_t> queue(1024);
    std::atomic<size_t> total_dequeued{0};
    std::atomic<size_t> producers_done{0};

    printf("moodycamel::ConcurrentQueue benchmark\n");
    printf("  producers: %d  consumers: %d  items/producer: %d\n\n", NUM_PRODUCERS, NUM_CONSUMERS, ITEMS_PER_PRODUCER);

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> prod_threads, con_threads;
    for (int t = 0; t < NUM_PRODUCERS; ++t) {
        prod_threads.emplace_back([&]() {
            moodycamel::ProducerToken ptok(queue);
            for (int i = 0; i < ITEMS_PER_PRODUCER; ++i)
                queue.enqueue(ptok, (uint64_t)i);
            producers_done.fetch_add(1, std::memory_order_release);
        });
    }
    for (int t = 0; t < NUM_CONSUMERS; ++t) {
        con_threads.emplace_back([&]() {
            moodycamel::ConsumerToken ctok(queue);
            size_t local = 0;
            uint64_t item;
            while (true) {
                if (queue.try_dequeue(ctok, item)) {
                    ++local;
                } else {
                    if (producers_done.load(std::memory_order_acquire) >= NUM_PRODUCERS) break;
                }
            }
            while (queue.try_dequeue(ctok, item)) ++local;
            total_dequeued.fetch_add(local, std::memory_order_relaxed);
        });
    }

    for (auto& t : prod_threads) t.join();
    for (auto& t : con_threads) t.join();

    uint64_t item;
    while (queue.try_dequeue(item)) total_dequeued.fetch_add(1, std::memory_order_relaxed);

    auto end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(end - start).count();
    double ops = (double)(NUM_PRODUCERS * ITEMS_PER_PRODUCER) / (ms / 1000.0);

    printf("results:\n");
    printf("  total:      %d\n", NUM_PRODUCERS * ITEMS_PER_PRODUCER);
    printf("  dequeued:   %zu\n", total_dequeued.load());
    printf("  elapsed:    %.2f ms\n", ms);
    printf("  throughput: %.0f ops/sec\n", ops);
}
