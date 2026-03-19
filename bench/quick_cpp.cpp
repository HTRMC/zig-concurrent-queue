#include "concurrentqueue.h"
#include <thread>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <vector>

int main() {
    constexpr int NP = 4, NC = 4, N = 1000000;
    moodycamel::ConcurrentQueue<uint64_t> q(4096);
    std::atomic<size_t> done{0}, total{0};

    auto t0 = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> pts, cts;
    for (int i = 0; i < NP; ++i) pts.emplace_back([&]{
        moodycamel::ProducerToken pt(q);
        for (int j = 0; j < N; ++j) q.enqueue(pt, (uint64_t)j);
        done.fetch_add(1, std::memory_order_release);
    });
    for (int i = 0; i < NC; ++i) cts.emplace_back([&]{
        moodycamel::ConsumerToken ct(q);
        uint64_t v; size_t local = 0;
        while (true) {
            if (q.try_dequeue(ct, v)) ++local;
            else if (done.load(std::memory_order_acquire) >= NP) break;
        }
        while (q.try_dequeue(ct, v)) ++local;
        total.fetch_add(local, std::memory_order_relaxed);
    });
    for (auto& t : pts) t.join();
    for (auto& t : cts) t.join();
    uint64_t v; while (q.try_dequeue(v)) total.fetch_add(1);

    auto t1 = std::chrono::high_resolution_clock::now();
    double s = std::chrono::duration<double>(t1-t0).count();
    printf("%.0f\n", (double)(NP*N)/s);
}
