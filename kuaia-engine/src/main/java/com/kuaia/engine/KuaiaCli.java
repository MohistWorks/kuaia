package com.kuaia.engine;

import com.kuaia.engine.benchmark.LocalPipelineBenchmarkRunner;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineConfigException;
import com.kuaia.engine.pipeline.PipelineConfigLoader;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import com.kuaia.engine.pipeline.PipelineRunSummary;

import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;

public class KuaiaCli {
    public static void main(String[] args) throws Exception {
        System.exit(run(args, System.out));
    }

    public static int run(String[] args, PrintStream out) throws Exception {
        if (args.length == 0 || "help".equals(args[0]) || "--help".equals(args[0]) || "-h".equals(args[0])) {
            printUsage(out);
            return 0;
        }

        String command = args[0];
        if ("local-demo".equals(command)) {
            new LocalPipelineRunner().run(out);
            return 0;
        }
        if ("ai-demo".equals(command)) {
            new AIDemoRunner().run(out);
            return 0;
        }
        if ("examples".equals(command)) {
            printExamples(out);
            return 0;
        }
        if ("benchmark".equals(command)) {
            try {
                LocalPipelineBenchmarkRunner.BenchmarkOptions options = parseBenchmarkOptions(args);
                new LocalPipelineBenchmarkRunner().run(options, out);
                return 0;
            } catch (IllegalArgumentException e) {
                out.println(e.getMessage());
                return 1;
            }
        }
        if ("recover-demo".equals(command)) {
            Path stateDir = parseStateDir(args, out);
            if (stateDir == null) {
                return 1;
            }
            new RecoveryDemoRunner().run(stateDir, out);
            return 0;
        }
        if ("run".equals(command)) {
            Path configPath = parseRunConfigPath(args, out);
            if (configPath == null) {
                return 1;
            }
            try {
                PipelineConfig config = new PipelineConfigLoader().load(configPath);
                PipelineRunSummary summary = new LocalPipelineRunner().run(config, out);
                out.println(summary.toCliLine());
                return 0;
            } catch (PipelineConfigException | PipelineExecutionException e) {
                out.println(e.getMessage());
                return 1;
            } catch (Exception e) {
                out.println(e.getMessage());
                return 1;
            }
        }

        out.println("Unknown command: " + command);
        printUsage(out);
        return 1;
    }

    private static Path parseStateDir(String[] args, PrintStream out) {
        for (int i = 1; i < args.length - 1; i++) {
            if ("--state-dir".equals(args[i])) {
                return Paths.get(args[i + 1]);
            }
        }
        out.println("recover-demo requires --state-dir <path>");
        printUsage(out);
        return null;
    }

    private static LocalPipelineBenchmarkRunner.BenchmarkOptions parseBenchmarkOptions(String[] args) {
        int rows = LocalPipelineBenchmarkRunner.DEFAULT_ROWS;
        int maxRowsPerSplit = LocalPipelineBenchmarkRunner.DEFAULT_MAX_ROWS_PER_SPLIT;
        int[] batchSizes = LocalPipelineBenchmarkRunner.DEFAULT_BATCH_SIZES;
        Path output = LocalPipelineBenchmarkRunner.DEFAULT_OUTPUT;
        for (int i = 1; i < args.length; i++) {
            String option = args[i];
            if ("--rows".equals(option)) {
                rows = parsePositiveIntOption(args, ++i, "--rows");
            } else if ("--max-rows-per-split".equals(option)) {
                maxRowsPerSplit = parseNonNegativeIntOption(args, ++i, "--max-rows-per-split");
            } else if ("--batch-sizes".equals(option)) {
                batchSizes = parseBatchSizes(requireOptionValue(args, ++i, "--batch-sizes"));
            } else if ("--output".equals(option)) {
                output = Paths.get(requireOptionValue(args, ++i, "--output"));
            } else {
                throw new IllegalArgumentException("Unknown benchmark option: " + option);
            }
        }
        return new LocalPipelineBenchmarkRunner.BenchmarkOptions(rows, maxRowsPerSplit, batchSizes, output);
    }

    private static int[] parseBatchSizes(String value) {
        String[] parts = value.split(",");
        int[] batchSizes = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i].trim();
            if (part.isEmpty()) {
                throw new IllegalArgumentException("benchmark --batch-sizes values must be integers: " + value);
            }
            try {
                batchSizes[i] = Integer.parseInt(part);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("benchmark --batch-sizes values must be integers: " + value, e);
            }
            if (batchSizes[i] <= 0) {
                throw new IllegalArgumentException("benchmark --batch-sizes values must be greater than zero");
            }
        }
        return batchSizes;
    }

    private static int parsePositiveIntOption(String[] args, int valueIndex, String optionName) {
        int value = parseIntOption(args, valueIndex, optionName);
        if (value <= 0) {
            throw new IllegalArgumentException("benchmark " + optionName + " must be greater than zero");
        }
        return value;
    }

    private static int parseNonNegativeIntOption(String[] args, int valueIndex, String optionName) {
        int value = parseIntOption(args, valueIndex, optionName);
        if (value < 0) {
            throw new IllegalArgumentException("benchmark " + optionName + " must not be negative");
        }
        return value;
    }

    private static int parseIntOption(String[] args, int valueIndex, String optionName) {
        String value = requireOptionValue(args, valueIndex, optionName);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("benchmark " + optionName + " must be an integer: " + value, e);
        }
    }

    private static String requireOptionValue(String[] args, int valueIndex, String optionName) {
        if (valueIndex >= args.length) {
            throw new IllegalArgumentException("benchmark " + optionName + " requires a value");
        }
        return args[valueIndex];
    }

    private static Path parseRunConfigPath(String[] args, PrintStream out) {
        for (int i = 1; i < args.length - 1; i++) {
            if ("-f".equals(args[i]) || "--file".equals(args[i])) {
                return Paths.get(args[i + 1]);
            }
        }
        out.println("run requires -f <pipeline.yaml>");
        printUsage(out);
        return null;
    }

    private static void printUsage(PrintStream out) {
        out.println("Usage: kuaia <command>");
        out.println();
        out.println("Commands:");
        out.println("  help                         Show this help message");
        out.println("  run -f PIPELINE              Run a declarative local pipeline");
        out.println("  local-demo                   Run FakeSource -> BinaryRow -> ConsoleSink");
        out.println("  ai-demo                      Run mock embedding -> mock vector sink");
        out.println("  examples                     Show runnable public example pipelines");
        out.println("  benchmark [options]          Run local batch benchmark");
        out.println("  recover-demo --state-dir DIR Demonstrate RocksDB task recovery");
        out.println();
        out.println("Examples:");
        out.println("  kuaia run -f examples/local-file-to-file.yaml");
        out.println("  kuaia run -f examples/local-file-to-vector.yaml");
        out.println("  kuaia benchmark --rows 10000");
    }

    private static void printExamples(PrintStream out) {
        out.println("Recommended no-service smoke:");
        out.println("  make public-mvp-smoke");
        out.println();
        out.println("No external services:");
        out.println("  kuaia run -f examples/local-file-to-console.yaml");
        out.println("  kuaia run -f examples/local-file-transform-to-console.yaml");
        out.println("  kuaia run -f examples/local-file-to-file.yaml");
        out.println("  kuaia run -f examples/local-file-to-vector.yaml");
        out.println("  kuaia run -f examples/local-file-skip-bad-records.yaml");
        out.println();
        out.println("External service examples:");
        out.println("  kuaia run -f examples/local-file-to-openai-compatible-vector.yaml");
        out.println("  kuaia run -f examples/local-file-to-qdrant.yaml");
        out.println("  kuaia run -f examples/postgres-to-qdrant.yaml");
        out.println();
        out.println("Docs:");
        out.println("  docs/examples.md");
        out.println("  docs/pipeline-yaml.md");
    }
}
