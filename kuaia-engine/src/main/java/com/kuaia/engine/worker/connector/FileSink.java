package com.kuaia.engine.worker.connector;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;

public class FileSink implements SinkWriter {
    private final KuaiaRowType rowType;
    private final Path path;
    private final String mode;
    private BufferedWriter writer;

    public FileSink(KuaiaRowType rowType, Path path, String mode) {
        this.rowType = rowType;
        this.path = path;
        this.mode = mode;
    }

    @Override
    public void open() throws Exception {
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        boolean append = "append".equals(mode);
        boolean writeHeader = !append || !Files.exists(path) || Files.size(path) == 0L;
        OpenOption[] options = append
                ? new OpenOption[]{
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.APPEND}
                : new OpenOption[]{
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING};
        writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8, options);
        if (writeHeader) {
            writer.write(String.join(",", rowType.getFieldNames()));
            writer.newLine();
        }
    }

    @Override
    public void write(BinaryRow row) throws Exception {
        StringBuilder line = new StringBuilder();
        DataType[] fieldTypes = rowType.getFieldTypes();
        for (int i = 0; i < fieldTypes.length; i++) {
            if (i > 0) {
                line.append(',');
            }
            line.append(formatValue(row, i, fieldTypes[i]));
        }
        writer.write(line.toString());
        writer.newLine();
    }

    @Override
    public void close() throws Exception {
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }

    private String formatValue(BinaryRow row, int field, DataType type) throws PipelineExecutionException {
        switch (type) {
            case LONG:
                return Long.toString(row.getLong(field));
            case STRING:
                return validateCsvCell(row.getString(field));
            case VECTOR:
                return formatVector(row.getVector(field));
            default:
                throw new PipelineExecutionException("File sink does not support field type: " + type);
        }
    }

    private String validateCsvCell(String value) throws PipelineExecutionException {
        if (value.indexOf(',') >= 0 || value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0) {
            throw new PipelineExecutionException("File sink does not support quoted CSV fields");
        }
        return value;
    }

    private String formatVector(float[] vector) {
        StringBuilder value = new StringBuilder("[");
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) {
                value.append(' ');
            }
            value.append(String.format(Locale.ROOT, "%.4f", vector[i]));
        }
        value.append(']');
        return value.toString();
    }
}
