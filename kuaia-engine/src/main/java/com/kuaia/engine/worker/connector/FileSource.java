package com.kuaia.engine.worker.connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FileSource implements LocalSource {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final Path path;
    private final String format;
    private KuaiaRowType rowType;
    private List<String> lines;
    private int firstDataLineIndex;

    public FileSource(Path path) {
        this(path, "csv");
    }

    public FileSource(Path path, String format) {
        this.path = path;
        this.format = format == null ? "csv" : format.trim().toLowerCase();
    }

    @Override
    public void open() throws Exception {
        if (!Files.exists(path)) {
            throw new PipelineExecutionException("Source file not found: " + path);
        }
        lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        if ("csv".equals(format)) {
            openCsv();
            return;
        }
        if ("jsonl".equals(format)) {
            openJsonl();
            return;
        }
        throw new PipelineExecutionException("Unsupported file source format: " + format);
    }

    private void openCsv() throws PipelineExecutionException {
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
        firstDataLineIndex = 1;
    }

    private void openJsonl() throws PipelineExecutionException {
        int schemaLineIndex = firstNonEmptyLineIndex();
        if (schemaLineIndex < 0) {
            throw new PipelineExecutionException("JSONL file is empty: " + path);
        }
        JsonNode schema = parseJsonObject(lines.get(schemaLineIndex), schemaLineIndex + 1);
        Iterator<String> fields = schema.fieldNames();
        List<String> fieldNames = new ArrayList<>();
        List<DataType> fieldTypes = new ArrayList<>();
        while (fields.hasNext()) {
            String fieldName = fields.next();
            if (fieldName == null || fieldName.trim().isEmpty()) {
                throw new PipelineExecutionException(
                        "Invalid JSONL row at line " + (schemaLineIndex + 1) + ": field name must not be empty");
            }
            JsonNode value = schema.get(fieldName);
            requireJsonScalar(value, schemaLineIndex + 1, fieldName);
            fieldNames.add(fieldName);
            fieldTypes.add(inferJsonDataType(fieldName, value));
        }
        if (fieldNames.isEmpty()) {
            throw new PipelineExecutionException(
                    "Invalid JSONL row at line " + (schemaLineIndex + 1) + ": expected at least one field");
        }
        rowType = new KuaiaRowType(
                fieldNames.toArray(new String[0]),
                fieldTypes.toArray(new DataType[0]));
        firstDataLineIndex = 0;
    }

    public int readAll(SinkWriter sink) throws Exception {
        return readFrom(0L, (seqId, row) -> sink.write(row));
    }

    public int readFrom(long lastCheckpointSeq, RecordConsumer consumer) throws Exception {
        return readFrom(lastCheckpointSeq, consumer, (seqId, error) -> false);
    }

    public long getRecordCount() {
        long count = 0L;
        for (int i = firstDataLineIndex; i < lines.size(); i++) {
            if (!lines.get(i).trim().isEmpty()) {
                count++;
            }
        }
        return count;
    }

    @Override
    public int readFrom(long lastCheckpointSeq, RecordConsumer consumer, RecordErrorConsumer errorConsumer)
            throws Exception {
        return readRange(lastCheckpointSeq, Long.MAX_VALUE, consumer, errorConsumer);
    }

    public int readRange(
            long lastCheckpointSeq,
            long endSeqInclusive,
            RecordConsumer consumer,
            RecordErrorConsumer errorConsumer) throws Exception {
        if (endSeqInclusive < 1L) {
            throw new IllegalArgumentException("endSeqInclusive must be greater than zero");
        }
        int count = 0;
        long seqId = 0L;
        for (int i = firstDataLineIndex; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.trim().isEmpty()) {
                continue;
            }
            seqId++;
            if (seqId <= lastCheckpointSeq) {
                continue;
            }
            if (seqId > endSeqInclusive) {
                break;
            }
            String[] fieldNames = rowType.getFieldNames();
            DataType[] fieldTypes = rowType.getFieldTypes();
            int lineNumber = i + 1;
            BinaryRow row;
            try {
                row = parseRow(line, fieldNames, fieldTypes, lineNumber);
            } catch (PipelineExecutionException e) {
                if (errorConsumer.accept(seqId, e)) {
                    continue;
                }
                throw e;
            }
            consumer.accept(seqId, row);
            count++;
        }
        return count;
    }

    @Override
    public void close() {
        lines = null;
    }

    @Override
    public KuaiaRowType getRowType() {
        return rowType;
    }

    private BinaryRow parseRow(String line, String[] fieldNames, DataType[] fieldTypes, int lineNumber)
            throws PipelineExecutionException {
        if ("jsonl".equals(format)) {
            return parseJsonlRow(line, fieldNames, fieldTypes, lineNumber);
        }
        return parseCsvRow(split(line), fieldNames, fieldTypes, lineNumber);
    }

    private BinaryRow parseCsvRow(String[] values, String[] fieldNames, DataType[] fieldTypes, int lineNumber)
            throws PipelineExecutionException {
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
                row.setLong(field, parseLong(values[field], lineNumber, fieldNames[field], "CSV"));
            } else {
                row.setString(field, values[field]);
            }
        }
        return row;
    }

    private BinaryRow parseJsonlRow(String line, String[] fieldNames, DataType[] fieldTypes, int lineNumber)
            throws PipelineExecutionException {
        JsonNode object = parseJsonObject(line, lineNumber);
        rejectUnexpectedJsonFields(object, lineNumber);
        BinaryRow row = new BinaryRow(fieldNames.length);
        for (int field = 0; field < fieldNames.length; field++) {
            JsonNode value = object.get(fieldNames[field]);
            if (value == null) {
                throw new PipelineExecutionException("Invalid JSONL row at line "
                        + lineNumber
                        + ": missing field "
                        + fieldNames[field]);
            }
            requireJsonScalar(value, lineNumber, fieldNames[field]);
            if (fieldTypes[field] == DataType.LONG) {
                row.setLong(field, parseLong(value.asText(), lineNumber, fieldNames[field], "JSONL"));
            } else {
                row.setString(field, value.asText());
            }
        }
        return row;
    }

    private JsonNode parseJsonObject(String line, int lineNumber) throws PipelineExecutionException {
        try {
            JsonNode node = JSON_MAPPER.readTree(line);
            if (node == null || !node.isObject()) {
                throw new PipelineExecutionException(
                        "Invalid JSONL row at line " + lineNumber + ": expected JSON object");
            }
            return node;
        } catch (JsonProcessingException e) {
            throw new PipelineExecutionException(
                    "Invalid JSONL row at line " + lineNumber + ": malformed JSON",
                    e);
        }
    }

    private void rejectUnexpectedJsonFields(JsonNode object, int lineNumber) throws PipelineExecutionException {
        Iterator<String> fields = object.fieldNames();
        while (fields.hasNext()) {
            String fieldName = fields.next();
            if (rowType.getIndex(fieldName) < 0) {
                throw new PipelineExecutionException("Invalid JSONL row at line "
                        + lineNumber
                        + ": unexpected field "
                        + fieldName);
            }
        }
    }

    private void requireJsonScalar(JsonNode value, int lineNumber, String fieldName) throws PipelineExecutionException {
        if (value == null) {
            throw new PipelineExecutionException("Invalid JSONL row at line "
                    + lineNumber
                    + ": missing field "
                    + fieldName);
        }
        if (value.isObject() || value.isArray()) {
            throw new PipelineExecutionException("Invalid JSONL row at line "
                    + lineNumber
                    + ": field "
                    + fieldName
                    + " must be a scalar value");
        }
        if (value.isNull()) {
            throw new PipelineExecutionException("Invalid JSONL row at line "
                    + lineNumber
                    + ": field "
                    + fieldName
                    + " must not be null");
        }
    }

    private DataType inferJsonDataType(String fieldName, JsonNode value) {
        if ("id".equals(fieldName) || value.isIntegralNumber()) {
            return DataType.LONG;
        }
        return DataType.STRING;
    }

    private long parseLong(String value, int lineNumber, String fieldName, String formatName)
            throws PipelineExecutionException {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new PipelineExecutionException(
                    "Invalid "
                            + formatName
                            + " row at line "
                            + lineNumber
                            + ": field "
                            + fieldName
                            + " is not a long",
                    e);
        }
    }

    private int firstNonEmptyLineIndex() {
        for (int i = 0; i < lines.size(); i++) {
            if (!lines.get(i).trim().isEmpty()) {
                return i;
            }
        }
        return -1;
    }

    private String[] split(String line) {
        return line.split(",", -1);
    }
}
