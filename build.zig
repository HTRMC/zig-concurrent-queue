const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lib_mod = b.createModule(.{
        .root_source_file = b.path("src/concurrent_queue.zig"),
        .target = target,
        .optimize = optimize,
    });

    const lib = b.addLibrary(.{
        .name = "concurrent-queue",
        .root_module = lib_mod,
    });
    b.installArtifact(lib);

    const tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/concurrent_queue.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_tests.step);

    const bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    bench_mod.addImport("concurrent-queue", lib_mod);
    const bench_exe = b.addExecutable(.{
        .name = "bench",
        .root_module = bench_mod,
    });
    bench_exe.linkLibC();
    b.installArtifact(bench_exe);

    const run_bench = b.addRunArtifact(bench_exe);
    const bench_step = b.step("bench", "Run benchmarks");
    bench_step.dependOn(&run_bench.step);

    const qbench_mod = b.createModule(.{
        .root_source_file = b.path("bench/quick_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    qbench_mod.addImport("concurrent-queue", lib_mod);
    const qbench_exe = b.addExecutable(.{
        .name = "qbench",
        .root_module = qbench_mod,
    });
    qbench_exe.linkLibC();
    b.installArtifact(qbench_exe);
    const run_qbench = b.addRunArtifact(qbench_exe);
    const qbench_step = b.step("qbench", "Quick single-number benchmark");
    qbench_step.dependOn(&run_qbench.step);

    const layout_mod = b.createModule(.{
        .root_source_file = b.path("bench/layout.zig"),
        .target = target,
        .optimize = optimize,
    });
    layout_mod.addImport("concurrent-queue", lib_mod);
    const layout_exe = b.addExecutable(.{
        .name = "layout",
        .root_module = layout_mod,
    });
    const run_layout = b.addRunArtifact(layout_exe);
    const layout_step = b.step("layout", "Print struct layout and cache line analysis");
    layout_step.dependOn(&run_layout.step);

    const asm_mod = b.createModule(.{
        .root_source_file = b.path("bench/asm_driver.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    asm_mod.addImport("concurrent-queue", lib_mod);
    const asm_emit = b.addLibrary(.{
        .name = "concurrent-queue-asm",
        .root_module = asm_mod,
    });
    asm_emit.root_module.strip = false;
    const asm_install = b.addInstallFile(asm_emit.getEmittedAsm(), "asm/concurrent_queue.s");
    const asm_step = b.step("asm", "Emit assembly for hot-path inspection");
    asm_step.dependOn(&asm_install.step);
}
