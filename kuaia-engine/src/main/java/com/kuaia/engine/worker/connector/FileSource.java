package com.kuaia.engine.worker.connector;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class FileSource {
    private final Path path;
    private KuaiaRowType rowType;
    private List<String> lines;

    public interface RecordConsumer {
        void accept(long seqId, BinaryRow row) throws Exception;
    }

    public FileSource(Path path) {
        this.path = path;
    }

    public void open() throws Exception {
        if (!Files.exists(path)) {
            throw new PipelineExecutionException("Source file not found: " + path);
        }
        lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            throw new PipelineExecutionException("CSV file is empty: " + path);
        }
        String[] headers = split(lines.get(0));
        if (headers.length == 0) {
            throw new PipelineExecutionException("CSV header is empty: " + path);
        }
        DataType[] fieldTypes = new DataType[headers.length];
        for (int i = 0; i < headers.length; i++) {
            headers[i] = headers[i].trim();
            fieldTypes[i] = "id".equals(headers[i]) ? DataType.LONG : DataType.STRING;
        }
        rowType = new KuaiaRowType(headers, fieldTypes);
    }

    public int readAll(SinkWriter sink) throws Exception {
        return readFrom(0L, (seqId, row) -> sink.write(row));
    }

    public int readFrom(long lastCheckpointSeq, RecordConsumer consumer) throws Exception {
        int count = 0;
        long seqId = 0L;
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.trim().isEmpty()) {
                continue;
            }
            seqId++;
            if (seqId <= lastCheckpointSeq) {
                continue;
            }
            String[] values = split(line);
            String[] fieldNames = rowType.getFieldNames();
            DataType[] fieldTypes = rowType.getFieldTypes();
            int lineNumber = i + 1;
            if (values.length != fieldNames.length) {
                throw new PipelineExecutionException("Invalid CSV row at line "
                        + lineNumber
                        + ": expected "
                        + fieldNames.length
                        + " columns but found "
                        + values.length);
            }
            BinaryRow row = new BinaryRow(fieldNames.length);
            for (int field = 0; field < fieldNames.length; field++) {
                if (fieldTypes[field] == DataType.LONG) {
                    row.setLong(field, parseLong(values[field], lineNumber, fieldNames[field]));
                } else {
                    row.setString(field, values[field]);
                }
            }
            consumer.accept(seqId, row);
            count++;
        }
        return count;
    }

    public void close() {
        lines = null;
    }

    public KuaiaRowType getRowType() {
        return rowType;
    }

    private long parseLong(String value, int lineNumber, String fieldName) throws PipelineExecutionException {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new PipelineExecutionException(
                    "Invalid CSV row at line " + lineNumber + ": field " + fieldName + " is not a long",
                    e);
        }
    }

    private String[] split(String line) {
        return line.split(",", -1);
    }
}
