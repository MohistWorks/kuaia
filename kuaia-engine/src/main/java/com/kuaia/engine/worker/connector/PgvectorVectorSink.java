package com.kuaia.engine.worker.connector;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class PgvectorVectorSink implements SinkWriter {
    private static final int DEFAULT_TIMEOUT_MILLIS = 30_000;
    private static final String IDENTIFIER = "[A-Za-z_][A-Za-z0-9_]*";

    private final KuaiaRowType rowType;
    private final int idOrdinal;
    private final int vectorOrdinal;
    private final int[] payloadOrdinals;
    private final String url;
    private final String userEnv;
    private final String passwordEnv;
    private final Map<String, String> environment;
    private final int timeoutMillis;
    private final String upsertSql;
    private final ConnectionFactory connectionFactory;

    private Connection connection;

    public PgvectorVectorSink(KuaiaRowType rowType, PipelineConfig.SinkConfig config)
            throws PipelineExecutionException {
        this(rowType, config, System.getenv());
    }

    PgvectorVectorSink(KuaiaRowType rowType, PipelineConfig.SinkConfig config, Map<String, String> environment)
            throws PipelineExecutionException {
        this(rowType, config, environment, PgvectorVectorSink::openJdbcConnection);
    }

    PgvectorVectorSink(
            KuaiaRowType rowType,
            PipelineConfig.SinkConfig config,
            Map<String, String> environment,
            ConnectionFactory connectionFactory)
            throws PipelineExecutionException {
        if (config == null) {
            throw new PipelineExecutionException("Missing pgvector sink config");
        }
        this.rowType = rowType;
        this.idOrdinal = requireField(rowType, config.getIdField(), DataType.LONG);
        this.vectorOrdinal = requireField(rowType, config.getVectorField(), DataType.VECTOR);
        this.payloadOrdinals = resolvePayloadOrdinals(
                rowType,
                config.getPayloadFields(),
                idOrdinal,
                vectorOrdinal);
        this.url = requireValue(config.getUrl(), "sink.url");
        this.userEnv = requireValue(config.getUserEnv(), "sink.userEnv");
        this.passwordEnv = requireValue(config.getPasswordEnv(), "sink.passwordEnv");
        this.environment = environment;
        this.timeoutMillis = config.getTimeoutMs() > 0 ? config.getTimeoutMs() : DEFAULT_TIMEOUT_MILLIS;
        this.upsertSql = buildUpsertSql(config.getTable());
        this.connectionFactory = connectionFactory;
    }

    @Override
    public void open() throws PipelineExecutionException {
        Properties properties = new Properties();
        properties.setProperty("user", requireEnv(userEnv, environment));
        properties.setProperty("password", requireEnv(passwordEnv, environment));
        String timeoutSeconds = Integer.toString(Math.max(1, (timeoutMillis + 999) / 1000));
        properties.setProperty("connectTimeout", timeoutSeconds);
        properties.setProperty("socketTimeout", timeoutSeconds);
        try {
            connection = connectionFactory.open(url, properties);
        } catch (SQLException e) {
            throw new PipelineExecutionException("Pgvector connection failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void write(BinaryRow row) throws Exception {
        writeBatch(Collections.singletonList(row));
    }

    @Override
    public void writeBatch(List<BinaryRow> rows) throws Exception {
        if (rows.isEmpty()) {
            return;
        }
        if (connection == null) {
            throw new PipelineExecutionException("Pgvector sink is not open");
        }
        try (PreparedStatement statement = connection.prepareStatement(upsertSql)) {
            for (BinaryRow row : rows) {
                bindRow(statement, row);
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException e) {
            throw new PipelineExecutionException("Pgvector upsert failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws Exception {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } finally {
            connection = null;
        }
    }

    private String buildUpsertSql(String table) throws PipelineExecutionException {
        String tableSql = tableIdentifier(requireValue(table, "sink.table"));
        String idField = fieldIdentifier(rowType.getFieldNames()[idOrdinal], "sink.idField");
        String vectorField = fieldIdentifier(rowType.getFieldNames()[vectorOrdinal], "sink.vectorField");
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(tableSql).append(" (").append(idField).append(", ").append(vectorField);
        for (int ordinal : payloadOrdinals) {
            sql.append(", ").append(fieldIdentifier(rowType.getFieldNames()[ordinal], "sink.payloadFields"));
        }
        sql.append(") VALUES (?, ?::vector");
        for (int ignored : payloadOrdinals) {
            sql.append(", ?");
        }
        sql.append(") ON CONFLICT (").append(idField).append(") DO UPDATE SET ");
        sql.append(vectorField).append(" = EXCLUDED.").append(vectorField);
        for (int ordinal : payloadOrdinals) {
            String field = fieldIdentifier(rowType.getFieldNames()[ordinal], "sink.payloadFields");
            sql.append(", ").append(field).append(" = EXCLUDED.").append(field);
        }
        return sql.toString();
    }

    private void bindRow(PreparedStatement statement, BinaryRow row) throws SQLException, PipelineExecutionException {
        statement.setLong(1, row.getLong(idOrdinal));
        statement.setString(2, vectorLiteral(row.getVector(vectorOrdinal)));
        DataType[] types = rowType.getFieldTypes();
        for (int i = 0; i < payloadOrdinals.length; i++) {
            int ordinal = payloadOrdinals[i];
            int parameter = i + 3;
            if (types[ordinal] == DataType.LONG) {
                statement.setLong(parameter, row.getLong(ordinal));
            } else if (types[ordinal] == DataType.STRING) {
                statement.setString(parameter, row.getString(ordinal));
            } else {
                throw new PipelineExecutionException(
                        "Pgvector sink does not support payload field type: " + types[ordinal].name());
            }
        }
    }

    private String vectorLiteral(float[] vector) {
        StringBuilder literal = new StringBuilder();
        literal.append("[");
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) {
                literal.append(",");
            }
            literal.append(Float.toString(vector[i]));
        }
        literal.append("]");
        return literal.toString();
    }

    private int requireField(KuaiaRowType rowType, String field, DataType type) throws PipelineExecutionException {
        int ordinal = rowType.getIndex(field);
        if (ordinal < 0 || rowType.getFieldTypes()[ordinal] != type) {
            throw new PipelineExecutionException("Pgvector sink requires " + type.name() + " field: " + field);
        }
        fieldIdentifier(rowType.getFieldNames()[ordinal], "sink." + field);
        return ordinal;
    }

    private int[] resolvePayloadOrdinals(
            KuaiaRowType rowType,
            List<String> payloadFields,
            int idOrdinal,
            int vectorOrdinal) throws PipelineExecutionException {
        String[] names = rowType.getFieldNames();
        DataType[] types = rowType.getFieldTypes();
        if (payloadFields == null || payloadFields.isEmpty()) {
            List<Integer> ordinals = new ArrayList<>();
            for (int i = 0; i < names.length; i++) {
                if (i != idOrdinal && i != vectorOrdinal) {
                    validatePayloadField(names[i], types[i]);
                    ordinals.add(i);
                }
            }
            return toIntArray(ordinals);
        }

        List<Integer> ordinals = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (String field : payloadFields) {
            if (!seen.add(field)) {
                throw new PipelineExecutionException("Duplicate pgvector payload field: " + field);
            }
            int ordinal = rowType.getIndex(field);
            if (ordinal < 0) {
                throw new PipelineExecutionException("Pgvector sink requires payload field: " + field);
            }
            if (ordinal == idOrdinal) {
                throw new PipelineExecutionException("Pgvector payload field must not be the id field: " + field);
            }
            if (ordinal == vectorOrdinal) {
                throw new PipelineExecutionException("Pgvector payload field must not be the vector field: " + field);
            }
            validatePayloadField(names[ordinal], types[ordinal]);
            ordinals.add(ordinal);
        }
        return toIntArray(ordinals);
    }

    private void validatePayloadField(String field, DataType type) throws PipelineExecutionException {
        fieldIdentifier(field, "sink.payloadFields");
        if (type != DataType.LONG && type != DataType.STRING) {
            throw new PipelineExecutionException(
                    "Pgvector sink does not support payload field type: " + type.name());
        }
    }

    private int[] toIntArray(List<Integer> values) {
        int[] result = new int[values.size()];
        for (int i = 0; i < values.size(); i++) {
            result[i] = values.get(i);
        }
        return result;
    }

    private String requireValue(String value, String field) throws PipelineExecutionException {
        if (value == null || value.trim().isEmpty()) {
            throw new PipelineExecutionException("Missing required field: " + field);
        }
        return value.trim();
    }

    private String requireEnv(String envName, Map<String, String> environment) throws PipelineExecutionException {
        String value = envName == null ? null : environment.get(envName);
        if (value == null || value.trim().isEmpty()) {
            throw new PipelineExecutionException("Missing pgvector environment variable: " + envName);
        }
        return value;
    }

    private String tableIdentifier(String table) throws PipelineExecutionException {
        String[] parts = table.split("\\.", -1);
        for (String part : parts) {
            fieldIdentifier(part, "sink.table");
        }
        return table;
    }

    private String fieldIdentifier(String field, String configField) throws PipelineExecutionException {
        if (field == null || !field.matches(IDENTIFIER)) {
            throw new PipelineExecutionException("Invalid " + configField + " identifier: " + field);
        }
        return field;
    }

    private static Connection openJdbcConnection(String url, Properties properties) throws SQLException {
        return DriverManager.getConnection(url, properties);
    }

    interface ConnectionFactory {
        Connection open(String url, Properties properties) throws SQLException;
    }
}
