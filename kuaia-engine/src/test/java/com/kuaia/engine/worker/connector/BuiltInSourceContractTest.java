package com.kuaia.engine.worker.connector;

import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.UnaryOperator;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BuiltInSourceContractTest {
    @TempDir
    Path tempDir;

    @Test
    void builtInSourcesHonorSeqIdCheckpointAndRowTypeContract() throws Exception {
        FakeJdbcDriver driver = new FakeJdbcDriver();
        DriverManager.registerDriver(driver);
        try {
            for (SourceCase sourceCase : sourceCases()) {
                assertFullReadContract(sourceCase);
                assertCheckpointReadContract(sourceCase);
            }
        } finally {
            DriverManager.deregisterDriver(driver);
        }
    }

    private void assertFullReadContract(SourceCase sourceCase) throws Exception {
        try (LocalSource source = sourceCase.create()) {
            source.open();
            int contentOrdinal = assertRowTypeContract(sourceCase.name, source.getRowType());
            CapturedRows rows = readRows(sourceCase, source, 0L, contentOrdinal);

            assertEquals(2, rows.size(), sourceCase.name);
            assertEquals(Arrays.asList(1L, 2L), rows.seqIds(), sourceCase.name);
            assertEquals(Arrays.asList("Alpha", "Beta"), rows.contents(), sourceCase.name);
        }
    }

    private void assertCheckpointReadContract(SourceCase sourceCase) throws Exception {
        try (LocalSource source = sourceCase.create()) {
            source.open();
            int contentOrdinal = assertRowTypeContract(sourceCase.name, source.getRowType());
            CapturedRows rows = readRows(sourceCase, source, 1L, contentOrdinal);

            assertEquals(1, rows.size(), sourceCase.name);
            assertEquals(Collections.singletonList(2L), rows.seqIds(), sourceCase.name);
            assertEquals(Collections.singletonList("Beta"), rows.contents(), sourceCase.name);
        }
    }

    private int assertRowTypeContract(String name, KuaiaRowType rowType) {
        assertTrue(rowType.getFieldNames().length > 0, name);
        assertEquals(0, rowType.getIndex("id"), name);
        assertEquals(DataType.LONG, rowType.getFieldTypes()[0], name);
        int contentOrdinal = rowType.getIndex("content");
        assertTrue(contentOrdinal >= 0, name + " should expose a content field");
        assertEquals(DataType.STRING, rowType.getFieldTypes()[contentOrdinal], name);
        return contentOrdinal;
    }

    private CapturedRows readRows(SourceCase sourceCase, LocalSource source, long lastCheckpointSeq, int contentOrdinal)
            throws Exception {
        CapturedRows rows = new CapturedRows();
        int read = source.readFrom(
                lastCheckpointSeq,
                (seqId, row) -> rows.add(seqId, sourceCase.normalizeContent(row.getString(contentOrdinal))),
                (seqId, error) -> {
                    throw new AssertionError(
                            sourceCase.name + " emitted unexpected row error at seq " + seqId, error);
                });
        assertEquals(rows.size(), read, sourceCase.name);
        return rows;
    }

    private List<SourceCase> sourceCases() throws Exception {
        Path csv = tempDir.resolve("documents.csv");
        Files.write(csv, Arrays.asList(
                "id,content",
                "1,Alpha",
                "2,Beta"), StandardCharsets.UTF_8);

        Path docs = tempDir.resolve("docs");
        Files.createDirectories(docs);
        Files.write(docs.resolve("a.md"), "Alpha".getBytes(StandardCharsets.UTF_8));
        Files.write(docs.resolve("b.txt"), "Beta".getBytes(StandardCharsets.UTF_8));

        Path pdfDocs = tempDir.resolve("pdf-docs");
        Files.createDirectories(pdfDocs);
        DocumentSourceTest.writePdf(pdfDocs.resolve("a.pdf"), "Alpha");
        DocumentSourceTest.writePdf(pdfDocs.resolve("b.pdf"), "Beta");

        return Arrays.asList(
                new SourceCase("file", () -> new FileSource(csv)),
                new SourceCase("document", () -> new DocumentSource(docs, "auto")),
                // PDF text extraction appends a line separator after each text line; trim it away so the
                // contract compares the logical document content.
                new SourceCase("document-pdf", () -> new DocumentSource(pdfDocs, "auto"), String::trim),
                new SourceCase("s3", () -> new S3ObjectSource("kuaia-docs", "docs/", fakeObjectStore())),
                new SourceCase("duckdb", () -> new DuckDBSource(duckDbConfig())),
                new SourceCase("postgres", () -> new PostgresSource(
                        jdbcConfig("postgres", "jdbc:kuaia-contract-postgres:documents"),
                        env("KUAIA_POSTGRES_USER", "kuaia", "KUAIA_POSTGRES_PASSWORD", "secret"))),
                new SourceCase("mysql", () -> new MySQLSource(
                        jdbcConfig("mysql", "jdbc:kuaia-contract-mysql:documents"),
                        env("KUAIA_MYSQL_USER", "kuaia", "KUAIA_MYSQL_PASSWORD", "secret"))));
    }

    private PipelineConfig.SourceConfig duckDbConfig() {
        return new PipelineConfig.SourceConfig(
                "duckdb",
                null,
                null,
                "jdbc:duckdb:",
                null,
                null,
                "select 1 as id, 'Alpha' as content union all select 2 as id, 'Beta' as content order by id");
    }

    private PipelineConfig.SourceConfig jdbcConfig(String type, String url) {
        String prefix = "postgres".equals(type) ? "POSTGRES" : "MYSQL";
        return new PipelineConfig.SourceConfig(
                type,
                null,
                null,
                url,
                "KUAIA_" + prefix + "_USER",
                "KUAIA_" + prefix + "_PASSWORD",
                "select id, content from documents order by id");
    }

    private Map<String, String> env(String... keyValues) {
        Map<String, String> env = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            env.put(keyValues[i], keyValues[i + 1]);
        }
        return env;
    }

    private S3ObjectStore fakeObjectStore() {
        return new S3ObjectStore() {
            private final List<S3ObjectMetadata> objects = Arrays.asList(
                    new S3ObjectMetadata("docs/b.txt", 4L),
                    new S3ObjectMetadata("docs/a.md", 5L));

            @Override
            public List<S3ObjectMetadata> listObjects(String bucket, String prefix) {
                assertEquals("kuaia-docs", bucket);
                assertEquals("docs/", prefix);
                return objects;
            }

            @Override
            public String readUtf8Object(String bucket, String key) throws PipelineExecutionException {
                assertEquals("kuaia-docs", bucket);
                if ("docs/a.md".equals(key)) {
                    return "Alpha";
                }
                if ("docs/b.txt".equals(key)) {
                    return "Beta";
                }
                throw new PipelineExecutionException("Unexpected fake S3 object: " + key);
            }

            @Override
            public void close() {}
        };
    }

    private interface SourceFactory {
        LocalSource create() throws Exception;
    }

    private static final class SourceCase {
        private final String name;
        private final SourceFactory factory;
        private final UnaryOperator<String> contentNormalizer;

        private SourceCase(String name, SourceFactory factory) {
            this(name, factory, UnaryOperator.identity());
        }

        private SourceCase(String name, SourceFactory factory, UnaryOperator<String> contentNormalizer) {
            this.name = name;
            this.factory = factory;
            this.contentNormalizer = contentNormalizer;
        }

        private LocalSource create() throws Exception {
            return factory.create();
        }

        private String normalizeContent(String content) {
            return contentNormalizer.apply(content);
        }
    }

    private static final class CapturedRows {
        private final List<RowValue> rows = new java.util.ArrayList<>();

        private void add(long seqId, String content) {
            rows.add(new RowValue(seqId, content));
        }

        private int size() {
            return rows.size();
        }

        private List<Long> seqIds() {
            return rows.stream().map(row -> row.seqId).collect(Collectors.toList());
        }

        private List<String> contents() {
            return rows.stream().map(row -> row.content).collect(Collectors.toList());
        }
    }

    private static final class RowValue {
        private final long seqId;
        private final String content;

        private RowValue(long seqId, String content) {
            this.seqId = seqId;
            this.content = content;
        }
    }

    private static final class FakeJdbcDriver implements Driver {
        private final Object[][] rows = new Object[][]{
                {1L, "Alpha"},
                {2L, "Beta"}
        };

        @Override
        public Connection connect(String url, Properties info) {
            if (!acceptsURL(url)) {
                return null;
            }
            return proxy(Connection.class, (proxy, method, args) -> {
                if ("prepareStatement".equals(method.getName())) {
                    return preparedStatement();
                }
                return defaultValue(method);
            });
        }

        @Override
        public boolean acceptsURL(String url) {
            return url != null
                    && (url.startsWith("jdbc:kuaia-contract-postgres:")
                    || url.startsWith("jdbc:kuaia-contract-mysql:"));
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
                if ("executeQuery".equals(method.getName())) {
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
            return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
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
            return null;
        }
    }
}
