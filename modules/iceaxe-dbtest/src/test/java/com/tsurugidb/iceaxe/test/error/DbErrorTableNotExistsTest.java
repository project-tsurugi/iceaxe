package com.tsurugidb.iceaxe.test.error;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ResponseBox;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * table not exists test
 */
class DbErrorTableNotExistsTest extends DbTestTableTester {

    private static final int ATTEMPT_SIZE = ResponseBox.responseBoxSize() + 200;

    @BeforeAll
    static void beforeAll() throws IOException {
        var LOG = LoggerFactory.getLogger(DbErrorTableNotExistsTest.class);
        LOG.debug("init start");

        dropTestTable();

        LOG.debug("init end");
    }

    @Test
    void select() throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(SELECT_SQL)) {
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                LOG.trace("i={}", i);
                var e0 = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                    tm.execute((TsurugiTransactionAction) transaction -> {
                        var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                            transaction.executeAndGetList(ps);
                        });
                        assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
                        throw e;
                    });
                });
                assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e0);
//              assertContains("table_not_found test", e0.getMessage()); // TODO エラー詳細情報の確認
            }
        }
    }

    @Test
    void insert() throws IOException {
        var sql = "insert into " + TEST + "(" + TEST_COLUMNS + ") values(123, 456, 'abc')";
        var expected = "table_not_found table `test' is not found";
        statement(sql, expected);
    }

    @Test
    void update() throws IOException {
        var sql = "update " + TEST + " set bar = 0";
        var expected = "table_not_found test.";
        statement(sql, expected);
    }

    @Test
    void delete() throws IOException {
        var sql = "delete from " + TEST;
        var expected = "table_not_found test.";
        statement(sql, expected);
    }

    private void statement(String sql, String expected) throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                var e0 = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                    tm.execute((TsurugiTransactionAction) transaction -> {
                        var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                            transaction.executeAndGetCount(ps);
                        });
                        assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
                        throw e;
                    });
                });
                assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e0);
                assertContains(expected, e0.getMessage());
            }
        }
    }

    @Test
    void selectBind() throws IOException {
        var foo = TgVariable.ofInt4("foo");
        var sql = SELECT_SQL + " where foo=" + foo;
        var parameterMapping = TgParameterMapping.of(foo);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, parameterMapping)) {
            tm.execute((TsurugiTransactionAction) transaction -> {
                var parameter = TgParameterList.of(foo.bind(1));
                var e = assertThrowsExactly(TsurugiIOException.class, () -> {
                    transaction.executeAndGetList(ps, parameter);
                });
                assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
                assertContains("table_not_found test", e.getMessage());
            });
            tm.execute((TsurugiTransactionAction) transaction -> {
                var parameter = TgParameterList.of(foo.bind(1));
                var e = assertThrowsExactly(TsurugiIOException.class, () -> {
                    transaction.executeAndGetList(ps, parameter);
                });
                assertEqualsCode(IceaxeErrorCode.PS_LOW_ERROR, e);
                assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e.getCause());
            });
        }
    }

    @Test
    void insertBind() throws IOException {
        var entity = new TestEntity(123, 456, "abc");
        var expected = "table_not_found table `test' is not found";
        preparedStatement(INSERT_SQL, INSERT_MAPPING, entity, expected);
    }

    @Test
    void updateBind() throws IOException {
        var bar = TgVariable.ofInt8("bar");
        var sql = "update " + TEST + " set bar = " + bar;
        var mapping = TgParameterMapping.of(bar);
        var parameter = TgParameterList.of(bar.bind(0));
        var expected = "table_not_found test";
        preparedStatement(sql, mapping, parameter, expected);
    }

    @Test
    void deleteBind() throws IOException {
        var foo = TgVariable.ofInt4("foo");
        var sql = "delete from " + TEST + " where foo=" + foo;
        var mapping = TgParameterMapping.of(foo);
        var parameter = TgParameterList.of(foo.bind(1));
        var expected = "table_not_found test";
        preparedStatement(sql, mapping, parameter, expected);
    }

    private <P> void preparedStatement(String sql, TgParameterMapping<P> parameterMapping, P parameter, String expected) throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
            tm.execute((TsurugiTransactionAction) transaction -> {
                var e = assertThrowsExactly(TsurugiIOException.class, () -> {
                    transaction.executeAndGetCount(ps, parameter);
                });
                assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
                assertContains(expected, e.getMessage());
            });
            tm.execute((TsurugiTransactionAction) transaction -> {
                var e = assertThrowsExactly(TsurugiIOException.class, () -> {
                    transaction.executeAndGetCount(ps, parameter);
                });
                assertEqualsCode(IceaxeErrorCode.PS_LOW_ERROR, e);
                assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e.getCause());
            });
        }
    }
}
