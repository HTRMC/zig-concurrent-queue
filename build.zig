const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lib_mod = b.addModule("concurrent-queue", .{
        .root_source_file = b.path("src/concurrent_queue.zig"),
    });

    const lib = b.addStaticLibrary(.{
        .name = "concurrent-queue",
        .root_source_file = b.path("src/concurrent_queue.zig"),
        .target = target,
        .optimize = optimize,
    });
    b.installArtifact(lib);

    const tests = b.addTest(.{
        .root_source_file = b.path("src/concurrent_queue.zig"),
        .target = target,
        .optimize = optimize,
    });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_tests.step);

    const bench_exe = b.addExecutable(.{
        .name = "bench",
        .root_source_file = b.path("bench/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    bench_exe.root_module.addImport("concurrent-queue", lib_mod);
    b.installArtifact(bench_exe);

    const run_bench = b.addRunArtifact(bench_exe);
    const bench_step = b.step("bench", "Run benchmarks");
    bench_step.dependOn(&run_bench.step);

    const asm_emit = b.addStaticLibrary(.{
        .name = "concurrent-queue-asm",
        .root_source_file = b.path("src/concurrent_queue.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    asm_emit.root_module.strip = false;
    const asm_install = b.addInstallFile(asm_emit.getEmittedAsm(), "asm/concurrent_queue.s");
    const asm_step = b.step("asm", "Emit assembly for hot-path inspection");
    asm_step.dependOn(&asm_install.step);
}
