package com.kuaia.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kuaia.engine.benchmark.LocalPipelineBenchmarkRunner;
import com.kuaia.engine.coordinator.state.RocksDbStateStore;
import com.kuaia.engine.coordinator.state.StateStore;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineConfigException;
import com.kuaia.engine.pipeline.PipelineConfigLoader;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import com.kuaia.engine.pipeline.PipelineRunSummary;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class KuaiaCli {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

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
        if ("validate".equals(command)) {
            Path configPath = parsePipelineFileOption("validate", args, out);
            if (configPath == null) {
                return 1;
            }
            try {
                PipelineConfig config = new PipelineConfigLoader().load(configPath);
                new LocalPipelineValidator().validate(config, out);
                return 0;
            } catch (PipelineConfigException | PipelineExecutionException e) {
                out.println(e.getMessage());
                return 1;
            } catch (Exception e) {
                out.println(e.getMessage());
                return 1;
            }
        }
        if ("run".equals(command)) {
            RunOptions options = parseRunOptions(args, out);
            if (options == null) {
                return 1;
            }
            try {
                PipelineConfig config = new PipelineConfigLoader().load(options.configPath);
                PipelineRunSummary summary = new LocalPipelineRunner().run(config, out);
                out.println(summary.toCliLine());
                if (options.summaryJsonPath != null) {
                    writeRunSummaryJson(options.summaryJsonPath, config.getName(), summary);
                    out.println("Run Summary JSON: " + options.summaryJsonPath);
                }
                return 0;
            } catch (PipelineConfigException | PipelineExecutionException e) {
                out.println(e.getMessage());
                return 1;
            } catch (Exception e) {
                out.println(e.getMessage());
                return 1;
            }
        }

        if ("coordinator".equals(command)) {
            return runCoordinator(args, out);
        }
        if ("worker".equals(command)) {
            return runWorker(args, out);
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

    private static Path parsePipelineFileOption(String command, String[] args, PrintStream out) {
        Path configPath = null;
        for (int i = 1; i < args.length; i++) {
            String option = args[i];
            if ("-f".equals(option) || "--file".equals(option)) {
                if (i + 1 >= args.length) {
                    out.println(command + " requires -f <pipeline.yaml>");
                    printUsage(out);
                    return null;
                }
                configPath = Paths.get(args[++i]);
            } else {
                out.println("Unknown " + command + " option: " + option);
                printUsage(out);
                return null;
            }
        }
        if (configPath == null) {
            out.println(command + " requires -f <pipeline.yaml>");
            printUsage(out);
            return null;
        }
        return configPath;
    }

    private static LocalPipelineBenchmarkRunner.BenchmarkOptions parseBenchmarkOptions(String[] args) {
        int rows = LocalPipelineBenchmarkRunner.DEFAULT_ROWS;
        int maxRowsPerSplit = LocalPipelineBenchmarkRunner.DEFAULT_MAX_ROWS_PER_SPLIT;
        int[] batchSizes = LocalPipelineBenchmarkRunner.DEFAULT_BATCH_SIZES;
        String format = LocalPipelineBenchmarkRunner.DEFAULT_FORMAT;
        Path output = null;
        for (int i = 1; i < args.length; i++) {
            String option = args[i];
            if ("--rows".equals(option)) {
                rows = parsePositiveIntOption(args, ++i, "--rows");
            } else if ("--max-rows-per-split".equals(option)) {
                maxRowsPerSplit = parseNonNegativeIntOption(args, ++i, "--max-rows-per-split");
            } else if ("--batch-sizes".equals(option)) {
                batchSizes = parseBatchSizes(requireOptionValue(args, ++i, "--batch-sizes"));
            } else if ("--format".equals(option)) {
                format = parseBenchmarkFormat(requireOptionValue(args, ++i, "--format"));
            } else if ("--output".equals(option)) {
                output = Paths.get(requireOptionValue(args, ++i, "--output"));
            } else {
                throw new IllegalArgumentException("Unknown benchmark option: " + option);
            }
        }
        Path selectedOutput = output == null
                ? LocalPipelineBenchmarkRunner.BenchmarkOptions.defaultOutput(format)
                : output;
        return new LocalPipelineBenchmarkRunner.BenchmarkOptions(
                rows,
                maxRowsPerSplit,
                batchSizes,
                format,
                selectedOutput);
    }

    private static String parseBenchmarkFormat(String value) {
        String format = value.trim().toLowerCase(java.util.Locale.ROOT);
        if (!LocalPipelineBenchmarkRunner.FORMAT_JSON.equals(format)
                && !LocalPipelineBenchmarkRunner.FORMAT_CSV.equals(format)) {
            throw new IllegalArgumentException("benchmark --format must be json or csv: " + value);
        }
        return format;
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

    private static RunOptions parseRunOptions(String[] args, PrintStream out) {
        Path configPath = null;
        Path summaryJsonPath = null;
        for (int i = 1; i < args.length; i++) {
            String option = args[i];
            if ("-f".equals(option) || "--file".equals(option)) {
                if (i + 1 >= args.length) {
                    out.println("run requires -f <pipeline.yaml>");
                    printUsage(out);
                    return null;
                }
                configPath = Paths.get(args[++i]);
            } else if ("--summary-json".equals(option)) {
                if (i + 1 >= args.length) {
                    out.println("run --summary-json requires <path>");
                    printUsage(out);
                    return null;
                }
                summaryJsonPath = Paths.get(args[++i]);
            } else {
                out.println("Unknown run option: " + option);
                printUsage(out);
                return null;
            }
        }
        if (configPath == null) {
            out.println("run requires -f <pipeline.yaml>");
            printUsage(out);
            return null;
        }
        return new RunOptions(configPath, summaryJsonPath);
    }

    private static int runCoordinator(String[] args, PrintStream out) {
        Integer port = null;
        Path stateDir = null;
        Path submitPath = null;
        int maxParallelism = 4;
        long leaseMillis = 30_000L;
        for (int i = 1; i < args.length; i++) {
            String option = args[i];
            switch (option) {
                case "--port":
                    if (i + 1 >= args.length) {
                        out.println("coordinator requires --port <P>");
                        printUsage(out);
                        return 1;
                    }
                    try {
                        port = Integer.parseInt(args[++i]);
                    } catch (NumberFormatException e) {
                        out.println("coordinator --port must be a positive integer");
                        printUsage(out);
                        return 1;
                    }
                    break;
                case "--state-dir":
                    if (i + 1 >= args.length) {
                        out.println("coordinator requires --state-dir <DIR>");
                        printUsage(out);
                        return 1;
                    }
                    stateDir = Paths.get(args[++i]);
                    break;
                case "--submit":
                    if (i + 1 >= args.length) {
                        out.println("coordinator --submit requires <pipeline.yaml>");
                        printUsage(out);
                        return 1;
                    }
                    submitPath = Paths.get(args[++i]);
                    break;
                case "--max-parallelism":
                    if (i + 1 >= args.length) {
                        out.println("coordinator --max-parallelism requires <N>");
                        printUsage(out);
                        return 1;
                    }
                    try {
                        maxParallelism = Integer.parseInt(args[++i]);
                    } catch (NumberFormatException e) {
                        out.println("coordinator --max-parallelism must be a positive integer");
                        printUsage(out);
                        return 1;
                    }
                    if (maxParallelism <= 0) {
                        out.println("coordinator --max-parallelism must be a positive integer");
                        printUsage(out);
                        return 1;
                    }
                    break;
                case "--lease-millis":
                    if (i + 1 >= args.length) {
                        out.println("coordinator --lease-millis requires <M>");
                        printUsage(out);
                        return 1;
                    }
                    try {
                        leaseMillis = Long.parseLong(args[++i]);
                    } catch (NumberFormatException e) {
                        out.println("coordinator --lease-millis must be a positive integer");
                        printUsage(out);
                        return 1;
                    }
                    if (leaseMillis <= 0) {
                        out.println("coordinator --lease-millis must be a positive integer");
                        printUsage(out);
                        return 1;
                    }
                    break;
                default:
                    out.println("Unknown coordinator option: " + option);
                    printUsage(out);
                    return 1;
            }
        }
        if (port == null) {
            out.println("coordinator requires --port <P>");
            printUsage(out);
            return 1;
        }
        if (port <= 0) {
            out.println("coordinator --port must be a positive integer");
            printUsage(out);
            return 1;
        }
        if (stateDir == null) {
            out.println("coordinator requires --state-dir <DIR>");
            printUsage(out);
            return 1;
        }

        // Load the pipeline first (pure parse, no side effects) so a bad --submit fails before we
        // open the store or bind the port — never leave a coordinator listening with nothing to do.
        PipelineConfig pipeline = null;
        if (submitPath != null) {
            try {
                pipeline = new PipelineConfigLoader().load(submitPath);
            } catch (PipelineConfigException e) {
                out.println(e.getMessage());
                return 1;
            }
        }

        StateStore store;
        try {
            store = new RocksDbStateStore(stateDir);
        } catch (Exception e) {
            out.println("Failed to open state store: " + e.getMessage());
            return 1;
        }

        CoordinatorServer server = new CoordinatorServer(store, leaseMillis);
        try {
            if (pipeline != null) {
                server.submit(pipeline, maxParallelism);
            }
            server.start(port);
        } catch (Exception e) {
            out.println(e.getMessage());
            server.close();
            return 1;
        }
        out.println("Coordinator listening on port " + server.port());
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        try {
            server.awaitTermination();
            return 0;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            server.close();
            return 1;
        }
    }

    private static int runWorker(String[] args, PrintStream out) {
        String id = null;
        String coordinator = null;
        for (int i = 1; i < args.length; i++) {
            String option = args[i];
            switch (option) {
                case "--id":
                    if (i + 1 >= args.length) {
                        out.println("worker requires --id <ID>");
                        printUsage(out);
                        return 1;
                    }
                    id = args[++i];
                    break;
                case "--coordinator":
                    if (i + 1 >= args.length) {
                        out.println("worker requires --coordinator <HOST:PORT>");
                        printUsage(out);
                        return 1;
                    }
                    coordinator = args[++i];
                    break;
                default:
                    out.println("Unknown worker option: " + option);
                    printUsage(out);
                    return 1;
            }
        }
        if (id == null) {
            out.println("worker requires --id <ID>");
            printUsage(out);
            return 1;
        }
        if (coordinator == null) {
            out.println("worker requires --coordinator <HOST:PORT>");
            printUsage(out);
            return 1;
        }
        int colon = coordinator.lastIndexOf(':');
        if (colon <= 0 || colon == coordinator.length() - 1) {
            out.println("worker --coordinator must be HOST:PORT");
            printUsage(out);
            return 1;
        }
        String host = coordinator.substring(0, colon);
        int port;
        try {
            port = Integer.parseInt(coordinator.substring(colon + 1));
        } catch (NumberFormatException e) {
            out.println("worker --coordinator must be HOST:PORT");
            printUsage(out);
            return 1;
        }
        if (port <= 0) {
            out.println("worker --coordinator must be HOST:PORT");
            printUsage(out);
            return 1;
        }

        WorkerRunner runner = new WorkerRunner(id);
        runner.start(host, port);
        out.println("Worker " + id + " connecting to " + coordinator);
        Runtime.getRuntime().addShutdownHook(new Thread(runner::close));
        try {
            runner.awaitTermination();
            return 0;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            runner.close();
            return 1;
        }
    }

    private static void writeRunSummaryJson(Path outputPath, String pipelineName, PipelineRunSummary summary)
            throws PipelineExecutionException {
        ObjectNode json = JSON_MAPPER.createObjectNode();
        json.put("pipelineName", pipelineName);
        json.put("rowsRead", summary.getRowsRead());
        json.put("rowsWritten", summary.getRowsWritten());
        json.put("rowsFailed", summary.getRowsFailed());
        json.put("rowsSkipped", summary.getRowsSkipped());
        json.put("checkpointSeq", summary.getCheckpointSeq());
        json.put("taskState", summary.getTaskState().name());
        json.put("sourceSplits", summary.getSourceSplits());
        json.put("sinkBatches", summary.getSinkBatches());
        json.put("durationMs", summary.getDurationMillis());
        try {
            Path absolutePath = outputPath.toAbsolutePath().normalize();
            Path parent = absolutePath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.write(absolutePath, JSON_MAPPER.writeValueAsBytes(json));
        } catch (IOException e) {
            throw new PipelineExecutionException("Failed to write run summary JSON: " + outputPath, e);
        }
    }

    private static void printUsage(PrintStream out) {
        out.println("Usage: kuaia <command>");
        out.println();
        out.println("Commands:");
        out.println("  help                         Show this help message");
        out.println("  run -f PIPELINE [--summary-json PATH]");
        out.println("                               Run a declarative local pipeline");
        out.println("  validate -f PIPELINE         Validate a pipeline without running it");
        out.println("  local-demo                   Run FakeSource -> BinaryRow -> ConsoleSink");
        out.println("  ai-demo                      Run mock embedding -> mock vector sink");
        out.println("  examples                     Show runnable public example pipelines");
        out.println("  benchmark [options]          Run local batch benchmark");
        out.println("  recover-demo --state-dir DIR Demonstrate RocksDB task recovery");
        out.println("  coordinator --port P --state-dir DIR [--submit PIPELINE] [--max-parallelism N] [--lease-millis M]");
        out.println("                               Start a coordinator (gRPC server + dispatch loop)");
        out.println("  worker --id ID --coordinator HOST:PORT");
        out.println("                               Start a worker that executes dispatched tasks");
        out.println();
        out.println("Examples:");
        out.println("  kuaia run -f examples/local-file-to-file.yaml");
        out.println("  kuaia run -f examples/local-file-to-vector.yaml");
        out.println("  kuaia benchmark --rows 10000");
    }

    private static void printExamples(PrintStream out) {
        out.println("Recommended no-service smoke:");
        out.println("  make public-mvp-smoke");
        out.println("Recommended preflight:");
        out.println("  kuaia validate -f examples/local-file-to-file.yaml");
        out.println();
        out.println("No external services:");
        out.println("  kuaia run -f examples/local-file-to-console.yaml");
        out.println("  kuaia run -f examples/local-file-transform-to-console.yaml");
        out.println("  kuaia run -f examples/local-file-to-file.yaml");
        out.println("  kuaia run -f examples/local-quoted-csv-to-file.yaml");
        out.println("  kuaia run -f examples/local-jsonl-to-file.yaml");
        out.println("  kuaia run -f examples/local-file-to-vector.yaml");
        out.println("  kuaia run -f examples/local-jsonl-to-vector.yaml");
        out.println("  kuaia run -f examples/local-jsonl-chunk-to-vector.yaml");
        out.println("  kuaia run -f examples/local-faq-jsonl-to-vector.yaml");
        out.println("  kuaia run -f examples/local-file-skip-bad-records.yaml");
        out.println();
        out.println("Common RAG flows:");
        out.println("  Local document import: kuaia run -f examples/local-jsonl-chunk-to-vector.yaml");
        out.println("  Document directory to Qdrant: kuaia run -f examples/document-directory-to-qdrant.yaml");
        out.println("  FAQ import: kuaia run -f examples/local-faq-jsonl-to-vector.yaml");
        out.println("  DuckDB to Qdrant: kuaia run -f examples/duckdb-csv-to-qdrant.yaml");
        out.println("  S3 to Qdrant: kuaia run -f examples/s3-docs-to-qdrant.yaml");
        out.println("  Local file to Milvus: kuaia run -f examples/local-file-to-milvus.yaml");
        out.println("  Postgres to Qdrant: kuaia run -f examples/postgres-to-qdrant.yaml");
        out.println("  Postgres to pgvector: kuaia run -f examples/postgres-to-pgvector.yaml");
        out.println("  MySQL to Qdrant: kuaia run -f examples/mysql-to-qdrant.yaml");
        out.println();
        out.println("External service examples:");
        out.println("  kuaia run -f examples/local-file-to-openai-compatible-vector.yaml");
        out.println("  kuaia run -f examples/local-file-to-qdrant.yaml");
        out.println("  kuaia run -f examples/local-jsonl-chunk-to-qdrant.yaml");
        out.println("  kuaia run -f examples/document-directory-to-qdrant.yaml");
        out.println("  kuaia run -f examples/duckdb-csv-to-qdrant.yaml");
        out.println("  kuaia run -f examples/s3-docs-to-qdrant.yaml");
        out.println("  kuaia run -f examples/local-file-to-milvus.yaml");
        out.println("  kuaia run -f examples/postgres-to-qdrant.yaml");
        out.println("  kuaia run -f examples/postgres-to-pgvector.yaml");
        out.println("  kuaia run -f examples/mysql-to-qdrant.yaml");
        out.println();
        out.println("Docs:");
        out.println("  docs/examples.md");
        out.println("  docs/pipeline-yaml.md");
    }

    private static class RunOptions {
        private final Path configPath;
        private final Path summaryJsonPath;

        private RunOptions(Path configPath, Path summaryJsonPath) {
            this.configPath = configPath;
            this.summaryJsonPath = summaryJsonPath;
        }
    }
}
