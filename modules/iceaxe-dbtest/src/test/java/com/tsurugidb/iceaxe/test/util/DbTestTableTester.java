package com.tsurugidb.iceaxe.test.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.AfterAll;

import com.nautilus_technologies.tsubakuro.exception.DiagnosticCode;
import com.nautilus_technologies.tsubakuro.exception.SqlServiceCode;
import com.tsurugidb.iceaxe.result.TgEntityResultMapping;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TgEntityParameterMapping;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionManager.TsurugiTransactionConsumer;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

public class DbTestTableTester {

    /** test (table name) */
    public static final String TEST = "test";
    public static final String TEST_COLUMNS = "foo, bar, zzz";

    private static TsurugiSession staticSession;

    protected static TsurugiSession getSession() throws IOException {
        if (staticSession == null) {
            staticSession = DbTestConnector.createSession();
        }
        return staticSession;
    }

    @AfterAll
    static void afterAll() throws IOException {
        if (staticSession != null) {
            staticSession.close();
            staticSession = null;
        }
    }

    protected static void dropTestTable() throws IOException {
        var sql = "drop table " + TEST;
        try {
            executeDdl(getSession(), sql);
        } catch (IOException e) {
            var code = findDiagnosticCode(e);
            if (code == SqlServiceCode.ERR_TRANSLATOR_ERROR) { // table_not_found
                return;
            }
            throw e;
        }
    }

    protected static void createTestTable() throws IOException {
        var sql = "create table " + TEST //
                + "(" //
                + "  foo int," //
                + "  bar bigint," //
                + "  zzz varchar(10)," //
                + "  primary key(foo)" //
                + ")";
        executeDdl(getSession(), sql);
    }

    protected static void executeDdl(TsurugiSession session, String sql) throws IOException {
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            ps.executeAndGetCount(tm);
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
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            tm.execute((TsurugiTransactionConsumer) transaction -> {
                for (int i = 0; i < size; i++) {
                    var entity = createTestEntity(i);
                    ps.executeAndGetCount(transaction, entity);
                }
            });
        }
    }

    protected static TestEntity createTestEntity(int i) {
        return new TestEntity(i, i, Integer.toString(i));
    }

    protected static final TgEntityResultMapping<TestEntity> SELECT_MAPPING = TgResultMapping.of(TestEntity::new) //
            .int4("foo", TestEntity::setFoo) //
            .int8("bar", TestEntity::setBar) //
            .character("zzz", TestEntity::setZzz);

    // transaction manager

    protected static TsurugiTransactionManager createTransactionManagerOcc(TsurugiSession session) {
        return session.createTransactionManager(TgTxOption.ofOCC());
    }

    // assertion

    protected void assertEqualsCode(DiagnosticCode expected, Exception actual) {
        var code = findDiagnosticCode(actual);
        assertEquals(expected, code);
    }

    protected static DiagnosticCode findDiagnosticCode(Throwable t) {
        var e = findTransactionException(t);
        if (e != null) {
            return e.getDiagnosticCode();
        }
        return null;
    }

    protected static TsurugiTransactionException findTransactionException(Throwable t) {
        for (; t != null; t = t.getCause()) {
            if (t instanceof TsurugiTransactionException) {
                return (TsurugiTransactionException) t;
            }
        }
        return null;
    }

    protected void assertEqualsTestTable(TestEntity... expected) throws IOException {
        var expectedList = List.of(expected);
        assertEqualsTestTable(expectedList);
    }

    protected void assertEqualsTestTable(List<TestEntity> expected) throws IOException {
        var actual = selectAllFromTest();
        assertEquals(expected, actual);
    }

    protected List<TestEntity> selectAllFromTest() throws IOException {
        var sql = "select " + TEST_COLUMNS + "\n" //
                + "from " + TEST + "\n" //
                + "order by " + TEST_COLUMNS;
        var resultMapping = TgResultMapping.of(TestEntity::new) //
                .int4(TestEntity::setFoo) //
                .int8(TestEntity::setBar) //
                .character(TestEntity::setZzz);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, resultMapping)) {
            return ps.executeAndGetList(tm);
        }
    }

    protected int selectCountFromTest() throws IOException {
        var sql = "select count(*) from " + TEST;
        var resultMapping = TgResultMapping.of(record -> record.nextInt4());

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, resultMapping)) {
            return ps.executeAndFindRecord(tm).get();
        }
    }
}