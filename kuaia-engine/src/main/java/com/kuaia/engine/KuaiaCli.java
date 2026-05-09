package com.kuaia.engine;

import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineConfigException;
import com.kuaia.engine.pipeline.PipelineConfigLoader;
import com.kuaia.engine.pipeline.PipelineExecutionException;

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
                new LocalPipelineRunner().run(config, out);
                return 0;
            } catch (PipelineConfigException | PipelineExecutionException e) {
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
        out.println("  recover-demo --state-dir DIR Demonstrate RocksDB task recovery");
    }
}
