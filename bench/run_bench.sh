#!/bin/bash
ZIG_BENCH=$(find .zig-cache -name "bench.exe" 2>/dev/null | head -1)
RUNS=${1:-1000}
export SKIP_BLOCKING=1

echo "$RUNS runs, randomized order..."
echo ""

cpp_total=0
zig_total=0
cpp_count=0
zig_count=0

for i in $(seq 1 $RUNS); do
    coin=$((RANDOM % 2))
    if [ $coin -eq 0 ]; then
        val=$(bench/cpp_bench.exe 2>&1 | grep throughput | grep -o '[0-9]*' | head -1)
        cpp_total=$((cpp_total + val))
        cpp_count=$((cpp_count + 1))
    else
        val=$($ZIG_BENCH 2>&1 | grep throughput | grep -o '[0-9]*' | head -1)
        zig_total=$((zig_total + val))
        zig_count=$((zig_count + 1))
    fi

    if [ $((i % 100)) -eq 0 ]; then
        cpp_avg=$((cpp_total / cpp_count))
        zig_avg=$((zig_total / zig_count))
        echo "  $i/$RUNS  C++($cpp_count): $cpp_avg  Zig($zig_count): $zig_avg"
    fi
done

cpp_avg=$((cpp_total / cpp_count))
zig_avg=$((zig_total / zig_count))

echo ""
echo "=== FINAL ($RUNS runs, random order) ==="
echo "C++ ($cpp_count runs): $cpp_avg ops/sec"
echo "Zig ($zig_count runs): $zig_avg ops/sec"
python -c "print(f'Ratio: {$cpp_avg / $zig_avg:.3f}x')"
