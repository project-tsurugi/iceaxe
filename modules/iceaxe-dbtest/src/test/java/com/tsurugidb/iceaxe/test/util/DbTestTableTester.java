package com.tsurugidb.iceaxe.test.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.opentest4j.AssertionFailedError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.parameter.mapping.TgEntityParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.sql.result.mapping.TgEntityResultMapping;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.manager.event.TsurugiTmEventListener;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.debug.DebugClient;
import com.tsurugidb.tsubakuro.exception.CoreServiceCode;
import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;
import com.tsurugidb.tsubakuro.sql.exception.SqlExecutionException;

public class DbTestTableTester {
    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    /** test (table name) */
    public static final String TEST = "test";
    public static final String TEST_COLUMNS = "foo, bar, zzz";
    public static final int ZZZ_SIZE = 10;

    private static TsurugiSession staticSession;
    private static ExecutorService staticService;
    private static DebugClient staticDebugClient;

    protected static TsurugiSession getSession() throws IOException {
        synchronized (DbTestTableTester.class) {
            if (staticSession == null) {
                String baseLabel = DbTestConnector.getSessionLabel();
                String label = baseLabel + " (staticSession)";
                staticSession = DbTestConnector.createSession(label);
            }
        }
        return staticSession;
    }

    private static DebugClient getDebugClient() throws IOException, InterruptedException {
        synchronized (DbTestTableTester.class) {
            if (staticDebugClient == null) {
                var session = getSession();
                staticDebugClient = DebugClient.attach(session.getLowSession());
            }
        }
        return staticDebugClient;
    }

    protected static synchronized void closeStaticSession() throws IOException, InterruptedException, ServerException {
        try (var c1 = staticSession; var c2 = staticDebugClient) {
            // close only
        } finally {
            staticSession = null;
            staticDebugClient = null;
        }
    }

    @AfterAll
    static void testerAfterAll() throws IOException, InterruptedException, ServerException {
        try (var c1 = staticSession; var c2 = staticDebugClient; var c3 = (Closeable) () -> {
            if (staticService != null) {
                staticService.shutdownNow();
            }
        }) {
            // close only
        } finally {
            staticSession = null;
            staticDebugClient = null;
            staticService = null;
        }

        DbTestConnector.closeLeakSession();
    }

    private static final boolean START_END_LOG_INFO = true;

    protected static void logInitStart(Logger log, TestInfo info) {
        setSessionLabel(info, null, "init all");

        if (START_END_LOG_INFO) {
            log.info("init all start");
        } else {
            log.debug("init all start");
        }
        serverLog(log, null, "init all start");
    }

    protected static void logInitEnd(Logger log, TestInfo info) {
        setSessionLabel(info, null, "init all end");
        try {
            if (START_END_LOG_INFO) {
                log.info("init all end");
            } else {
                log.debug("init all end");
            }
            serverLog(log, null, "init all end");
        } finally {
            DbTestConnector.setSessionLabel(null);
        }
    }

    protected void logInitStart(TestInfo info) {
        String displayName = getDisplayName(info);
        setSessionLabel(info, displayName, "init");

        if (START_END_LOG_INFO) {
            LOG.info("{} init start", displayName);
        } else {
            LOG.debug("{} init start", displayName);
        }
        serverLog(LOG, displayName, "init start");
    }

    protected void logInitEnd(TestInfo info) {
        String displayName = getDisplayName(info);
        setSessionLabel(info, displayName, "init end");

        try {
            if (START_END_LOG_INFO) {
                LOG.info("{} init end", displayName);
            } else {
                LOG.debug("{} init end", displayName);
            }
            serverLog(LOG, displayName, "init end");
        } finally {
            DbTestConnector.setSessionLabel(null);
        }
    }

    @BeforeEach
    void tetsterBeforeEach(TestInfo info) {
        String displayName = getDisplayName(info);
        setSessionLabel(info, displayName, null);

        if (START_END_LOG_INFO) {
            LOG.info("{} start", displayName);
        } else {
            LOG.debug("{} start", displayName);
        }
        serverLog(LOG, displayName, "start");
    }

    @AfterEach
    void testerAfterEach(TestInfo info) {
        String displayName = getDisplayName(info);
        setSessionLabel(info, displayName, "end");

        try {
            if (START_END_LOG_INFO) {
                LOG.info("{} end", displayName);
            } else {
                LOG.debug("{} end", displayName);
            }
            serverLog(LOG, displayName, "end");
        } finally {
            DbTestConnector.setSessionLabel(null);
        }
    }

    private static void setSessionLabel(TestInfo info, String displayName, String suffix) {
        String className = info.getTestClass().map(c -> c.getSimpleName()).orElse("Unknown");
        String label = className;
        if (displayName != null) {
            label += "." + displayName;
        }
        if (suffix != null) {
            label += " " + suffix;
        }
        DbTestConnector.setSessionLabel(label);
    }

    private static String getDisplayName(TestInfo info) {
        String d = info.getDisplayName();
        String m = info.getTestMethod().map(Method::getName).orElse(null);
        if (m != null && !d.startsWith(m)) {
            return m + "() " + d;
        }
        return d;
    }

    protected static void serverLog(Logger log, String displayName, String message) {
        try {
            var client = getDebugClient();
            var m = "iceaxe-dbtest: " + getServerLogName(log, displayName) + " " + message;
            client.logging(m).await(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("serverLog error. message={}", message, e);
        }
    }

    private static String getServerLogName(Logger log, String displayName) {
        String name = log.getName();
        int n = name.lastIndexOf('.');
        if (n >= 0) {
            name = name.substring(n + 1);
        }

        if (displayName == null) {
            return name;
        }
        return name + "." + displayName;
    }

    // property

    protected static String getSystemProperty(String key, String defaultValue) {
        String property = System.getProperty(key);
        return (property != null) ? property : defaultValue;
    }

    protected static int getSystemProperty(String key, int defaultValue) {
        String property = getSystemProperty(key, (String) null);
        return (property != null) ? Integer.parseInt(property) : defaultValue;
    }

    // utility

    protected static void dropTestTable() throws IOException, InterruptedException {
        dropTable(TEST);
    }

    protected static void dropTable(String tableName) throws IOException, InterruptedException {
        if (existsTable(tableName)) {
            var sql = "drop table " + tableName;
            executeDdl(getSession(), sql, tableName);
        }
    }

    protected static boolean existsTable(String tableName) throws IOException, InterruptedException {
        var session = getSession();
        var opt = session.findTableMetadata(tableName);
        return opt.isPresent();
    }

    protected static final String CREATE_TEST_SQL = "create table " + TEST //
            + "(" //
            + "  foo int," //
            + "  bar bigint," //
            + "  zzz varchar(" + ZZZ_SIZE + ")," //
            + "  primary key(foo)" //
            + ")";
    protected static final String CREATE_TEST_NO_PK_SQL = "create table " + TEST //
            + "(" //
            + "  foo int," //
            + "  bar bigint," //
            + "  zzz varchar(" + ZZZ_SIZE + ")" //
            + ")";

    protected static void createTestTable() throws IOException, InterruptedException {
        createTestTable(true);
    }

    protected static void createTestTable(boolean hasPk) throws IOException, InterruptedException {
        var sql = hasPk ? CREATE_TEST_SQL : CREATE_TEST_NO_PK_SQL;
        executeDdl(getSession(), sql, TEST);
    }

    protected static void executeDdl(TsurugiSession session, String sql) throws IOException, InterruptedException {
        String tableName;
        if (sql.startsWith("create")) {
            int s = sql.indexOf("table");
            int e = sql.indexOf('(');
            tableName = sql.substring(s + "table".length(), e).trim();
        } else if (sql.startsWith("drop")) {
            int s = sql.indexOf("table");
            tableName = sql.substring(s + "table".length()).trim();
        } else {
            throw new IllegalArgumentException(sql);
        }
        executeDdl(session, sql, tableName);
    }

    protected static void executeDdl(TsurugiSession session, String sql, String tableName) throws IOException, InterruptedException {
        boolean workaround = false;
        if (workaround) {
            executeDdlWorkaround(session, sql, tableName);
            return;
        }

        var tm = createTransactionManagerOcc(session, "executeDdl", 1);
        try {
            tm.executeDdl(sql);
        } catch (TsurugiTmIOException e) {
            var log = LoggerFactory.getLogger(DbTestTableTester.class);
            log.error("executeDdl error. {}, sql={}, status={}", e.getMessage(), sql, e.getTransactionStatus());
            throw e;
        }
    }

    @Deprecated(forRemoval = true)
    private static void executeDdlWorkaround(TsurugiSession session, String sql, String tableName) throws IOException, InterruptedException {
        var tm = createTransactionManagerOcc(session, "executeDdlWorkaround", 3);
        tm.addEventListener(new TsurugiTmEventListener() {
            @Override
            public void transactionException(TsurugiTransaction transaction, Throwable e) {
                var log = LoggerFactory.getLogger(DbTestTableTester.class);
                log.warn("executeDdl error. {}, sql={}", e.getMessage(), sql);
            }
        });
        try (var ps = session.createStatement(sql)) {
            for (int i = 1;; i++) {
                try {
                    tm.executeAndGetCount(ps);
                    return;
                } catch (TsurugiTmIOException e) {
                    // duplicate_table（ERR_PHANTOM）が発生したら、リトライ
                    String message = e.getMessage();
                    if (message.contains("ERR_COMPILER_ERROR: SQL--0005") && message.contains("translating statement failed: duplicate_table table")) {
                        var line = Arrays.stream(e.getStackTrace()).filter(elem -> {
                            String fullName = elem.getClassName();
                            return fullName.startsWith("com.tsurugidb.iceaxe.test.") && fullName.endsWith("Test");
                        }).findFirst().orElse(null);
                        var log = LoggerFactory.getLogger(DbTestTableTester.class);
                        log.warn("executeDdl duplicate_table retry{} at {}", i, line);

                        // リトライ時にテーブルは消えているはずなので、自分でdropはしない
                        // dropTable(tableName);
                        continue;
                    }
                    throw e;
                }
            }
        }
    }

    protected static final String INSERT_SQL = "insert into " + TEST //
            + "(" + TEST_COLUMNS + ")" //
            + "values(:foo, :bar, :zzz)";
    private static final String UPSERT_SQL = "insert or replace into " + TEST //
            + "(" + TEST_COLUMNS + ")" //
            + "values(:foo, :bar, :zzz)";
    protected static final TgEntityParameterMapping<TestEntity> INSERT_MAPPING = TgParameterMapping.of(TestEntity.class) //
            .addInt("foo", TestEntity::getFoo) //
            .addLong("bar", TestEntity::getBar) //
            .addString("zzz", TestEntity::getZzz);

    protected static void insertTestTable(int size) throws IOException, InterruptedException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session, "insertTestTable", 3);
        try (var ps = session.createStatement(UPSERT_SQL, INSERT_MAPPING)) {
            tm.execute(transaction -> {
                for (int i = 0; i < size; i++) {
                    var entity = createTestEntity(i);
                    transaction.executeAndGetCount(ps, entity);
                }
                return;
            });
        }
    }

    protected static TestEntity createTestEntity(int i) {
        return new TestEntity(i, i, Integer.toString(i));
    }

    protected static void insertTestTable(TestEntity entity) throws IOException, InterruptedException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session, "insertTestTable", 3);
        try (var ps = session.createStatement(UPSERT_SQL, INSERT_MAPPING)) {
            tm.execute(transaction -> {
                transaction.executeAndGetCount(ps, entity);
            });
        }
    }

    protected static void insertTestTable(List<TestEntity> list) throws IOException, InterruptedException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session, "insertTestTable", 3);
        try (var ps = session.createStatement(UPSERT_SQL, INSERT_MAPPING)) {
            tm.execute(transaction -> {
                // TODO use Transaction.batch()
                var resultList = new ArrayList<TsurugiStatementResult>(list.size());
                for (var entity : list) {
                    var result = transaction.executeStatement(ps, entity);
                    resultList.add(result);
                }
                for (var result : resultList) {
                    result.checkLowResult();
                }
                return;
            });
        }
    }

    protected static final String SELECT_SQL = "select " + TEST_COLUMNS + " from " + TEST;

    protected static final TgEntityResultMapping<TestEntity> SELECT_MAPPING = TgResultMapping.of(TestEntity::new) //
            .addInt("foo", TestEntity::setFoo) //
            .addLong("bar", TestEntity::setBar) //
            .addString("zzz", TestEntity::setZzz);

    protected static <T> Future<T> executeFuture(Callable<T> task) {
        synchronized (DbTestTableTester.class) {
            if (staticService == null) {
                staticService = Executors.newCachedThreadPool();
            }
        }

        var started = new AtomicBoolean(false);
        var future = staticService.submit(() -> {
            started.set(true);
            return task.call();
        });
        while (!started.get()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return future;
    }

    // transaction manager

    protected static TsurugiTransactionManager createTransactionManagerOcc(TsurugiSession session) {
        return session.createTransactionManager(TgTxOption.ofOCC());
    }

    protected static TsurugiTransactionManager createTransactionManagerOcc(TsurugiSession session, int max) {
        return session.createTransactionManager(TgTmSetting.ofAlways(TgTxOption.ofOCC(), max));
    }

    protected static TsurugiTransactionManager createTransactionManagerOcc(TsurugiSession session, String txLabel, int max) {
        return session.createTransactionManager(TgTmSetting.ofAlways(TgTxOption.ofOCC().label(txLabel), max));
    }

    protected static TsurugiTransactionManager createTransactionManagerLtx(TsurugiSession session, String tableName) {
        return session.createTransactionManager(TgTxOption.ofLTX(tableName));
    }

    // assertion

    protected static void assertEqualsCode(DiagnosticCode expected, Throwable actual) {
        try {
            var code = findDiagnosticCode(actual);
            assertEquals(expected, code);

            var expectedClass = findLowServerExceptionClass(expected);
            if (expectedClass != null) {
                var actualServerException = findLowServerException(actual);
                assertEquals(expectedClass, actualServerException.getClass());
            }
        } catch (Throwable e) {
            var log = LoggerFactory.getLogger(DbTestTableTester.class);
            log.error("assertEqualsCode fail. {}", e.getMessage(), actual);

            e.addSuppressed(actual);
            throw e;
        }
    }

    protected static void assertEqualsCode(Set<DiagnosticCode> expected, Throwable actual) {
        try {
            var code = findDiagnosticCode(actual);
            if (expected.contains(code)) {
                return;
            }
            fail(String.format("expected: %s but was: %s", expected, actual));
        } catch (Throwable e) {
            var log = LoggerFactory.getLogger(DbTestTableTester.class);
            log.error("assertEqualsCode fail. {}", e.getMessage(), actual);

            e.addSuppressed(actual);
            throw e;
        }
    }

    protected static DiagnosticCode findDiagnosticCode(Throwable t) {
        {
            var e = findTsurugiDiagnosticCodeProvider(t);
            if (e != null) {
                return e.getDiagnosticCode();
            }
        }
        {
            var e = findLowServerException(t);
            if (e != null) {
                return e.getDiagnosticCode();
            }
        }
        return null;
    }

    protected static TsurugiDiagnosticCodeProvider findTsurugiDiagnosticCodeProvider(Throwable t) {
        for (; t != null; t = t.getCause()) {
            if (t instanceof TsurugiDiagnosticCodeProvider) {
                return (TsurugiDiagnosticCodeProvider) t;
            }
        }
        return null;
    }

    protected static ServerException findLowServerException(Throwable t) {
        for (; t != null; t = t.getCause()) {
            if (t instanceof ServerException) {
                return (ServerException) t;
            }
        }
        return null;
    }

    protected static Class<?> findLowServerExceptionClass(DiagnosticCode code) {
        if (code instanceof IceaxeErrorCode) {
            return null;
        }
        if (code instanceof CoreServiceCode) {
            return null;
        }
        if (code instanceof SqlServiceCode) {
            if (code == SqlServiceCode.SQL_SERVICE_EXCEPTION) {
                return SqlServiceException.class;
            }

            var name = toCamelCase(code.name());
            var className = SqlExecutionException.class.getPackage().getName() + "." + name;
            try {
                return Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new AssertionError(e);
            }
        }
        throw new AssertionError(code);
    }

    private static String toCamelCase(String snakeCase) {
        return Arrays.stream(snakeCase.split("_")).map(s -> Character.toUpperCase(s.charAt(0)) + s.substring(1).toLowerCase()).collect(Collectors.joining());
    }

    protected static void assertErrorTableNotFound(String expectedTableName, Exception actual) {
        assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, actual);

        String message = actual.getMessage();
        assertContains("compile failed with error:table_not_found", message);
        assertContains('"' + expectedTableName + '"', message);
    }

    protected static void assertErrorVariableNotFound(String expectedVariableName, Exception actual) {
        assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, actual);

        String message = actual.getMessage();
        assertContains("compile failed with error:variable_not_found", message);
        assertContains('"' + expectedVariableName + '"', message);
    }

    protected static void assertErrorIndexNotFound(String expectedIndexName, Exception actual) {
        assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, actual);

        String message = actual.getMessage();
        assertContains("compile failed with error:index_not_found", message);
        assertContains('"' + expectedIndexName + '"', message);
    }

    protected static void assertEqualsTestTable(TestEntity... expected) throws IOException, InterruptedException {
        var expectedList = List.of(expected);
        assertEqualsTestTable(expectedList);
    }

    protected static void assertEqualsTestTable(List<TestEntity> expected) throws IOException, InterruptedException {
        var actual = selectAllFromTest();
        assertEquals(expected, actual);
    }

    protected static void assertEqualsTestTable(int expectedSize) throws IOException, InterruptedException {
        assertEqualsTestTable(expectedSize, entity -> {
        });
    }

    protected static void assertEqualsTestTable(int expectedSize, Consumer<TestEntity> modifier) throws IOException, InterruptedException {
        var actualList = selectAllFromTest();
        assertEqualsTestTable(expectedSize, actualList, modifier);
    }

    protected static void assertEqualsTestTable(int expectedSize, List<TestEntity> actualList) {
        assertEqualsTestTable(expectedSize, actualList, entity -> {
        });
    }

    protected static void assertEqualsTestTable(int expectedSize, List<TestEntity> actualList, Consumer<TestEntity> modifier) {
        assertEquals(expectedSize, actualList.size());
        for (int i = 0; i < expectedSize; i++) {
            var expected = createTestEntity(i);
            modifier.accept(expected);
            var actual = actualList.get(i);
            assertEquals(expected, actual);
        }
    }

    protected static void assertEqualsResultEntity(TestEntity expected, TsurugiResultEntity actual) {
        assertEquals(expected.getFoo(), actual.getIntOrNull("foo"));
        assertEquals(expected.getBar(), actual.getLongOrNull("bar"));
        assertEquals(expected.getZzz(), actual.getStringOrNull("zzz"));
    }

    protected static List<TestEntity> selectAllFromTest() throws IOException, InterruptedException {
        return selectAllFromTest(TgTmSetting.ofAlways(TgTxOption.ofOCC().label("selectAllFromTest"), 3));
    }

    protected static List<TestEntity> selectAllFromTest(TgTmSetting setting) throws IOException, InterruptedException {
        var sql = SELECT_SQL + "\norder by " + TEST_COLUMNS;

        var session = getSession();
        var tm = session.createTransactionManager(setting);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            return tm.executeAndGetList(ps);
        }
    }

    protected static TestEntity selectFromTest(int foo) throws IOException, InterruptedException {
        var where1 = TgBindVariable.ofInt("foo");
        var sql = SELECT_SQL + " where foo=" + where1;
        var session = getSession();
        var tm = createTransactionManagerOcc(session, "selectFromTest", 3);
        try (var ps = session.createQuery(sql, TgParameterMapping.of(where1), SELECT_MAPPING)) {
            var parameter = TgBindParameters.of(where1.bind(foo));
            return tm.executeAndFindRecord(ps, parameter).orElse(null);
        }
    }

    protected static int selectCountFromTest() throws IOException, InterruptedException {
        return selectCountFrom(TEST);
    }

    protected static int selectCountFrom(String tableName) throws IOException, InterruptedException {
        var sql = "select count(*) from " + tableName;
        var resultMapping = TgResultMapping.ofSingle(int.class);

        var session = getSession();
        var tm = createTransactionManagerOcc(session, "selectCountFrom", 3);
        try (var ps = session.createQuery(sql, resultMapping)) {
            return tm.executeAndFindRecord(ps).get();
        }
    }

    protected static void assertContains(String expected, String actual) {
        assertNotNull(actual);
        if (actual.contains(expected)) {
            return; // success
        }
        assertEquals(expected, actual, "not contains");
    }

    protected static void assertMatches(String expectedRegexp, String actual) {
        assertNotNull(actual);
        if (Pattern.matches(expectedRegexp, actual)) {
            return; // success
        }
        assertEquals(expectedRegexp, actual, "unmatched");
    }

    protected static void assertEqualsMessage(String expected, Throwable actual) {
        try {
            assertEquals(expected, actual.getMessage());
        } catch (AssertionFailedError e) {
            e.addSuppressed(actual);
            throw e;
        }
    }

    protected static void assertUpdateCount(int expected, int actual) {
        assertEquals(expected, actual);
    }
}
