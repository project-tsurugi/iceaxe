package com.tsurugidb.iceaxe.test.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;
import com.tsurugidb.iceaxe.result.TgEntityResultMapping;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TgEntityParameterMapping;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.exception.ServerException;

public class DbTestTableTester {
    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    /** test (table name) */
    public static final String TEST = "test";
    public static final String TEST_COLUMNS = "foo, bar, zzz";
    public static final int ZZZ_SIZE = 10;

    private static TsurugiSession staticSession;

    protected static TsurugiSession getSession() throws IOException {
        if (staticSession == null) {
            staticSession = DbTestConnector.createSession();
        }
        return staticSession;
    }

    @AfterAll
    static void testerAfterAll() throws IOException {
        if (staticSession != null) {
            staticSession.close();
            staticSession = null;
        }
    }

    @BeforeEach
    void tetsterBeforeEach(TestInfo info) {
        LOG.debug("{} start", info.getDisplayName());
    }

    @AfterEach
    void testerAfterEach(TestInfo info) {
        LOG.debug("{} end", info.getDisplayName());
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

    protected static void dropTestTable() throws IOException {
        dropTable(TEST);
    }

    protected static void dropTable(String tableName) throws IOException {
        if (existsTable(tableName)) {
            var sql = "drop table " + tableName;
            executeDdl(getSession(), sql);
        }
    }

    protected static boolean existsTable(String tableName) throws IOException {
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

    protected static void createTestTable() throws IOException {
        executeDdl(getSession(), CREATE_TEST_SQL);
    }

    protected static void executeDdl(TsurugiSession session, String sql) throws IOException {
        boolean workaround = false;
        if (workaround) {
            executeDdlWorkaround(session, sql);
            return;
        }

        var tm = createTransactionManagerOcc(session);
        tm.executeDdl(sql);
    }

    @Deprecated(forRemoval = true)
    private static void executeDdlWorkaround(TsurugiSession session, String sql) throws IOException {
        var tm = createTransactionManagerOcc(session, 3);
        try (var ps = session.createPreparedStatement(sql)) {
            for (int i = 1;; i++) {
                try {
                    tm.executeAndGetCount(ps);
                    return;
                } catch (TsurugiTransactionIOException e) {
                    // duplicate_table（ERR_PHANTOM）が発生したら、リトライ
                    if (e.getMessage().contains("ERR_COMPILER_ERROR: SQL--0005: translating statement failed: duplicate_table table `test' is already defined")) {
                        var line = Arrays.stream(e.getStackTrace()).filter(elem -> {
                            String fullName = elem.getClassName();
                            return fullName.startsWith("com.tsurugidb.iceaxe.test.") && fullName.endsWith("Test");
                        }).findFirst().orElse(null);
                        var log = LoggerFactory.getLogger(DbTestTableTester.class);
                        log.warn("executeDdl duplicate_table retry{} at {}", i, line);

                        dropTestTable();
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
    protected static final TgEntityParameterMapping<TestEntity> INSERT_MAPPING = TgParameterMapping.of(TestEntity.class) //
            .int4("foo", TestEntity::getFoo) //
            .int8("bar", TestEntity::getBar) //
            .character("zzz", TestEntity::getZzz);

    protected static void insertTestTable(int size) throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session, 3);
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            tm.execute((TsurugiTransactionAction) transaction -> {
                for (int i = 0; i < size; i++) {
                    var entity = createTestEntity(i);
                    transaction.executeAndGetCount(ps, entity);
                }
            });
        }
    }

    protected static TestEntity createTestEntity(int i) {
        return new TestEntity(i, i, Integer.toString(i));
    }

    protected static void insertTestTable(TestEntity entity) throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session, 3);
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            tm.execute((TsurugiTransactionAction) transaction -> {
                transaction.executeAndGetCount(ps, entity);
            });
        }
    }

    protected static final String SELECT_SQL = "select " + TEST_COLUMNS + " from " + TEST;

    protected static final TgEntityResultMapping<TestEntity> SELECT_MAPPING = TgResultMapping.of(TestEntity::new) //
            .int4("foo", TestEntity::setFoo) //
            .int8("bar", TestEntity::setBar) //
            .character("zzz", TestEntity::setZzz);

    // transaction manager

    protected static TsurugiTransactionManager createTransactionManagerOcc(TsurugiSession session) {
        return session.createTransactionManager(TgTxOption.ofOCC());
    }

    protected static TsurugiTransactionManager createTransactionManagerOcc(TsurugiSession session, int max) {
        return session.createTransactionManager(TgTmSetting.ofAlways(TgTxOption.ofOCC(), 3));
    }

    // assertion

    protected void assertEqualsCode(DiagnosticCode expected, Throwable actual) {
        var code = findDiagnosticCode(actual);
        assertEquals(expected, code);
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

    protected static void assertEqualsTestTable(TestEntity... expected) throws IOException {
        var expectedList = List.of(expected);
        assertEqualsTestTable(expectedList);
    }

    protected static void assertEqualsTestTable(List<TestEntity> expected) throws IOException {
        var actual = selectAllFromTest();
        assertEquals(expected, actual);
    }

    protected static void assertEqualsTestTable(int expectedSize) throws IOException {
        var actualList = selectAllFromTest();
        assertEqualsTestTable(expectedSize, actualList);
    }

    protected static void assertEqualsTestTable(int expectedSize, List<TestEntity> actualList) {
        assertEquals(expectedSize, actualList.size());
        for (int i = 0; i < expectedSize; i++) {
            var expected = createTestEntity(i);
            var actual = actualList.get(i);
            assertEquals(expected, actual);
        }
    }

    protected static void assertEqualsResultEntity(TestEntity expected, TsurugiResultEntity actual) {
        assertEquals(expected.getFoo(), actual.getInt4OrNull("foo"));
        assertEquals(expected.getBar(), actual.getInt8OrNull("bar"));
        assertEquals(expected.getZzz(), actual.getCharacterOrNull("zzz"));
    }

    protected static List<TestEntity> selectAllFromTest() throws IOException {
        var sql = SELECT_SQL + "\norder by " + TEST_COLUMNS;

        var session = getSession();
        var tm = createTransactionManagerOcc(session, 3);
        try (var ps = session.createPreparedQuery(sql, SELECT_MAPPING)) {
            return tm.executeAndGetList(ps);
        }
    }

    protected static TestEntity selectFromTest(int foo) throws IOException {
        var where1 = TgVariable.ofInt4("foo");
        var sql = SELECT_SQL + " where foo=" + where1;
        var session = getSession();
        var tm = createTransactionManagerOcc(session, 3);
        try (var ps = session.createPreparedQuery(sql, TgParameterMapping.of(where1), SELECT_MAPPING)) {
            var parameter = TgParameterList.of(where1.bind(foo));
            return tm.executeAndFindRecord(ps, parameter).orElse(null);
        }
    }

    protected static int selectCountFromTest() throws IOException {
        return selectCountFrom(TEST);
    }

    protected static int selectCountFrom(String tableName) throws IOException {
        var sql = "select count(*) from " + tableName;
        var resultMapping = TgResultMapping.of(record -> record.nextInt4());

        var session = getSession();
        var tm = createTransactionManagerOcc(session, 3);
        try (var ps = session.createPreparedQuery(sql, resultMapping)) {
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

    protected static void assertUpdateCount(int expected, int actual) {
        assertEquals(-1, actual); // TODO use expected (for updateCount)
    }
}
