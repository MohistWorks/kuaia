package com.kuaia.engine.worker.connector;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import java.util.Properties;

public class PostgresSource implements LocalSource {
    private final PipelineConfig.SourceConfig config;
    private final String user;
    private final String password;

    private Connection connection;
    private PreparedStatement statement;
    private ResultSet resultSet;
    private KuaiaRowType rowType;

    public PostgresSource(PipelineConfig.SourceConfig config) throws PipelineExecutionException {
        this(config, System.getenv());
    }

    PostgresSource(PipelineConfig.SourceConfig config, Map<String, String> environment)
            throws PipelineExecutionException {
        this.config = config;
        this.user = requireEnv(config.getUserEnv(), environment);
        this.password = requireEnv(config.getPasswordEnv(), environment);
    }

    @Override
    public void open() throws PipelineExecutionException {
        try {
            Properties properties = new Properties();
            properties.setProperty("user", user);
            properties.setProperty("password", password);
            connection = DriverManager.getConnection(config.getUrl(), properties);
            statement = connection.prepareStatement(config.getQuery());
            if (config.getFetchSize() > 0) {
                statement.setFetchSize(config.getFetchSize());
            }
            resultSet = statement.executeQuery();
            rowType = buildRowType(resultSet.getMetaData());
        } catch (SQLException e) {
            closeQuietly();
            throw new PipelineExecutionException("Postgres source query failed: " + e.getMessage(), e);
        }
    }

    @Override
    public int readFrom(long lastCheckpointSeq, RecordConsumer consumer, RecordErrorConsumer errorConsumer)
            throws Exception {
        int count = 0;
        long seqId = 0L;
        while (resultSet.next()) {
            seqId++;
            if (seqId <= lastCheckpointSeq) {
                continue;
            }
            BinaryRow row;
            try {
                row = readRow(seqId);
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
    public KuaiaRowType getRowType() {
        return rowType;
    }

    @Override
    public void close() throws Exception {
        SQLException failure = null;
        failure = close(resultSet, failure);
        failure = close(statement, failure);
        failure = close(connection, failure);
        resultSet = null;
        statement = null;
        connection = null;
        if (failure != null) {
            throw failure;
        }
    }

    private KuaiaRowType buildRowType(ResultSetMetaData metaData) throws SQLException {
        int fieldCount = metaData.getColumnCount();
        String[] fieldNames = new String[fieldCount];
        DataType[] fieldTypes = new DataType[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            int column = i + 1;
            String label = metaData.getColumnLabel(column);
            if (label == null || label.trim().isEmpty()) {
                label = metaData.getColumnName(column);
            }
            fieldNames[i] = label;
            fieldTypes[i] = mapType(metaData.getColumnType(column));
        }
        return new KuaiaRowType(fieldNames, fieldTypes);
    }

    private BinaryRow readRow(long seqId) throws SQLException, PipelineExecutionException {
        BinaryRow row = new BinaryRow(rowType.getFieldNames().length);
        for (int i = 0; i < rowType.getFieldNames().length; i++) {
            int column = i + 1;
            if (rowType.getFieldTypes()[i] == DataType.LONG) {
                long value = resultSet.getLong(column);
                if (resultSet.wasNull()) {
                    throw new PipelineExecutionException(
                            "Invalid Postgres row seq=" + seqId + ": field " + rowType.getFieldNames()[i] + " is null");
                }
                row.setLong(i, value);
            } else {
                String value = resultSet.getString(column);
                row.setString(i, value == null ? "" : value);
            }
        }
        return row;
    }

    private DataType mapType(int sqlType) {
        switch (sqlType) {
            case Types.BIGINT:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                return DataType.LONG;
            default:
                return DataType.STRING;
        }
    }

    private String requireEnv(String envName, Map<String, String> environment) throws PipelineExecutionException {
        String value = envName == null ? null : environment.get(envName);
        if (value == null || value.trim().isEmpty()) {
            throw new PipelineExecutionException("Missing Postgres environment variable: " + envName);
        }
        return value;
    }

    private SQLException close(AutoCloseable closeable, SQLException failure) {
        if (closeable == null) {
            return failure;
        }
        try {
            closeable.close();
            return failure;
        } catch (Exception e) {
            if (failure != null) {
                return failure;
            }
            if (e instanceof SQLException) {
                return (SQLException) e;
            }
            return new SQLException(e);
        }
    }

    private void closeQuietly() {
        try {
            close();
        } catch (Exception ignored) {
            // Preserve the original query failure.
        }
    }
}
