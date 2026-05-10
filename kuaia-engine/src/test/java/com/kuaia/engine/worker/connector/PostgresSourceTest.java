package com.kuaia.engine.worker.connector;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PostgresSourceTest {
    private FakeJdbcDriver driver;

    @AfterEach
    void deregisterDriver() throws Exception {
        if (driver != null) {
            DriverManager.deregisterDriver(driver);
        }
    }

    @Test
    void readsRowsFromJdbcQuery() throws Exception {
        driver = new FakeJdbcDriver();
        DriverManager.registerDriver(driver);
        PostgresSource source = new PostgresSource(sourceConfig(), env(
                "KUAIA_POSTGRES_USER", "kuaia",
                "KUAIA_POSTGRES_PASSWORD", "secret"));

        source.open();
        List<BinaryRow> rows = new ArrayList<>();
        int count = source.readFrom(0L, (seqId, row) -> rows.add(row), (seqId, error) -> false);
        source.close();

        assertEquals(2, count);
        assertEquals("jdbc:kuaia-postgres-test:documents", driver.url);
        assertEquals("kuaia", driver.user);
        assertEquals("secret", driver.password);
        assertEquals("select id, content from documents order by id", driver.query);

        KuaiaRowType rowType = source.getRowType();
        assertEquals("id", rowType.getFieldNames()[0]);
        assertEquals("content", rowType.getFieldNames()[1]);
        assertEquals(DataType.LONG, rowType.getFieldTypes()[0]);
        assertEquals(DataType.STRING, rowType.getFieldTypes()[1]);
        assertEquals(1L, rows.get(0).getLong(0));
        assertEquals("Alpha", rows.get(0).getString(1));
        assertEquals(2L, rows.get(1).getLong(0));
        assertEquals("Beta", rows.get(1).getString(1));
    }

    @Test
    void skipsRowsBeforeCheckpoint() throws Exception {
        driver = new FakeJdbcDriver();
        DriverManager.registerDriver(driver);
        PostgresSource source = new PostgresSource(sourceConfig(), env(
                "KUAIA_POSTGRES_USER", "kuaia",
                "KUAIA_POSTGRES_PASSWORD", "secret"));

        source.open();
        List<BinaryRow> rows = new ArrayList<>();
        int count = source.readFrom(1L, (seqId, row) -> rows.add(row), (seqId, error) -> false);
        source.close();

        assertEquals(1, count);
        assertEquals(2L, rows.get(0).getLong(0));
        assertEquals("Beta", rows.get(0).getString(1));
    }

    @Test
    void appliesConfiguredFetchSizeToPreparedStatement() throws Exception {
        driver = new FakeJdbcDriver();
        DriverManager.registerDriver(driver);
        PostgresSource source = new PostgresSource(sourceConfigWithFetchSize(128), env(
                "KUAIA_POSTGRES_USER", "kuaia",
                "KUAIA_POSTGRES_PASSWORD", "secret"));

        source.open();
        source.close();

        assertEquals(128, driver.fetchSize);
    }

    @Test
    void rejectsMissingPasswordEnvironmentVariable() {
        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> new PostgresSource(sourceConfig(), env("KUAIA_POSTGRES_USER", "kuaia")));

        assertEquals("Missing Postgres environment variable: KUAIA_POSTGRES_PASSWORD", error.getMessage());
    }

    @Test
    void rejectsQueryFailure() throws Exception {
        driver = new FakeJdbcDriver();
        driver.queryFailure = new SQLException("boom");
        DriverManager.registerDriver(driver);
        PostgresSource source = new PostgresSource(sourceConfig(), env(
                "KUAIA_POSTGRES_USER", "kuaia",
                "KUAIA_POSTGRES_PASSWORD", "secret"));

        PipelineExecutionException error = assertThrows(PipelineExecutionException.class, source::open);
        source.close();

        assertEquals("Postgres source query failed: boom", error.getMessage());
    }

    private PipelineConfig.SourceConfig sourceConfig() {
        return new PipelineConfig.SourceConfig(
                "postgres",
                null,
                null,
                "jdbc:kuaia-postgres-test:documents",
                "KUAIA_POSTGRES_USER",
                "KUAIA_POSTGRES_PASSWORD",
                "select id, content from documents order by id");
    }

    private PipelineConfig.SourceConfig sourceConfigWithFetchSize(int fetchSize) {
        return new PipelineConfig.SourceConfig(
                "postgres",
                null,
                null,
                "jdbc:kuaia-postgres-test:documents",
                "KUAIA_POSTGRES_USER",
                "KUAIA_POSTGRES_PASSWORD",
                "select id, content from documents order by id",
                0,
                fetchSize);
    }

    private Map<String, String> env(String... keyValues) {
        Map<String, String> env = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            env.put(keyValues[i], keyValues[i + 1]);
        }
        return env;
    }

    private static class FakeJdbcDriver implements Driver {
        private final Object[][] rows = new Object[][]{
                {1L, "Alpha"},
                {2L, "Beta"}
        };
        private String url;
        private String user;
        private String password;
        private String query;
        private int fetchSize;
        private SQLException queryFailure;

        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            if (!acceptsURL(url)) {
                return null;
            }
            this.url = url;
            this.user = info.getProperty("user");
            this.password = info.getProperty("password");
            return proxy(Connection.class, (proxy, method, args) -> {
                if ("prepareStatement".equals(method.getName())) {
                    query = (String) args[0];
                    return preparedStatement();
                }
                return defaultValue(method);
            });
        }

        @Override
        public boolean acceptsURL(String url) {
            return url != null && url.startsWith("jdbc:kuaia-postgres-test:");
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }

        private PreparedStatement preparedStatement() {
            return proxy(PreparedStatement.class, (proxy, method, args) -> {
                if ("setFetchSize".equals(method.getName())) {
                    fetchSize = (Integer) args[0];
                    return null;
                }
                if ("executeQuery".equals(method.getName())) {
                    if (queryFailure != null) {
                        throw queryFailure;
                    }
                    return resultSet();
                }
                return defaultValue(method);
            });
        }

        private ResultSet resultSet() {
            ResultSetMetaData metaData = metaData();
            return proxy(ResultSet.class, new InvocationHandler() {
                private int index = -1;

                @Override
                public Object invoke(Object proxy, Method method, Object[] args) {
                    if ("next".equals(method.getName())) {
                        index++;
                        return index < rows.length;
                    }
                    if ("getMetaData".equals(method.getName())) {
                        return metaData;
                    }
                    if ("getLong".equals(method.getName())) {
                        return rows[index][((Integer) args[0]) - 1];
                    }
                    if ("getString".equals(method.getName())) {
                        Object value = rows[index][((Integer) args[0]) - 1];
                        return value == null ? null : value.toString();
                    }
                    if ("wasNull".equals(method.getName())) {
                        return false;
                    }
                    return defaultValue(method);
                }
            });
        }

        private ResultSetMetaData metaData() {
            return proxy(ResultSetMetaData.class, (proxy, method, args) -> {
                if ("getColumnCount".equals(method.getName())) {
                    return 2;
                }
                if ("getColumnLabel".equals(method.getName()) || "getColumnName".equals(method.getName())) {
                    return ((Integer) args[0]) == 1 ? "id" : "content";
                }
                if ("getColumnType".equals(method.getName())) {
                    return ((Integer) args[0]) == 1 ? Types.BIGINT : Types.VARCHAR;
                }
                return defaultValue(method);
            });
        }

        @SuppressWarnings("unchecked")
        private static <T> T proxy(Class<T> type, InvocationHandler handler) {
            return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class[]{type}, handler);
        }

        private static Object defaultValue(Method method) {
            Class<?> returnType = method.getReturnType();
            if (returnType == Boolean.TYPE) {
                return false;
            }
            if (returnType == Integer.TYPE) {
                return 0;
            }
            if (returnType == Long.TYPE) {
                return 0L;
            }
            if (returnType == Void.TYPE) {
                return null;
            }
            return null;
        }
    }
}
