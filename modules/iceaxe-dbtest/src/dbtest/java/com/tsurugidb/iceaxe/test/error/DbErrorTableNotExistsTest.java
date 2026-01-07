package com.tsurugidb.iceaxe.test.error;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.Link;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * table not exists test
 */
class DbErrorTableNotExistsTest extends DbTestTableTester {

    private static final int ATTEMPT_SIZE = Link.responseBoxSize() + 200;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbErrorTableNotExistsTest.class);
        logInitStart(LOG, info);

        dropTestTable();

        logInitEnd(LOG, info);
    }

    @Test
    void select() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(SELECT_SQL)) {
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                LOG.trace("i={}", i);
                var e0 = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                    tm.execute((TsurugiTransactionAction) transaction -> {
                        var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                            transaction.executeAndGetList(ps);
                        });
                        assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e);
                        throw e;
                    });
                });
                assertErrorTableNotFound(TEST, e0);
            }
        }
    }

    @Test
    void insert() throws Exception {
        var sql = "insert into " + TEST + "(" + TEST_COLUMNS + ") values(123, 456, 'abc')";
        statement(sql);
    }

    @Test
    void update() throws Exception {
        var sql = "update " + TEST + " set bar = 0";
        statement(sql);
    }

    @Test
    void delete() throws Exception {
        var sql = "delete from " + TEST;
        statement(sql);
    }

    private void statement(String sql) throws IOException, InterruptedException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                var e0 = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                    tm.execute((TsurugiTransactionAction) transaction -> {
                        var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                            transaction.executeAndGetCount(ps);
                        });
                        assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e);
                        throw e;
                    });
                });
                assertErrorTableNotFound(TEST, e0);
            }
        }
    }

    @Test
    void selectBind() throws Exception {
        var foo = TgBindVariable.ofInt("foo");
        var sql = SELECT_SQL + " where foo=" + foo;
        var parameterMapping = TgParameterMapping.of(foo);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, parameterMapping)) {
            tm.execute(transaction -> {
                var parameter = TgBindParameters.of(foo.bind(1));
                var e = assertThrowsExactly(TsurugiIOException.class, () -> {
                    transaction.executeAndGetList(ps, parameter);
                });
                assertErrorTableNotFound(TEST, e);
            });
            tm.execute(transaction -> {
                var parameter = TgBindParameters.of(foo.bind(1));
                var e = assertThrowsExactly(IceaxeIOException.class, () -> {
                    transaction.executeAndGetList(ps, parameter);
                });
                assertEqualsCode(IceaxeErrorCode.PS_LOW_ERROR, e);
                assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e.getCause());
            });
        }
    }

    @Test
    void insertBind() throws Exception {
        var entity = new TestEntity(123, 456, "abc");
        preparedStatement(INSERT_SQL, INSERT_MAPPING, entity);
    }

    @Test
    void updateBind() throws Exception {
        var bar = TgBindVariable.ofLong("bar");
        var sql = "update " + TEST + " set bar = " + bar;
        var mapping = TgParameterMapping.of(bar);
        var parameter = TgBindParameters.of(bar.bind(0));
        preparedStatement(sql, mapping, parameter);
    }

    @Test
    void deleteBind() throws Exception {
        var foo = TgBindVariable.ofInt("foo");
        var sql = "delete from " + TEST + " where foo=" + foo;
        var mapping = TgParameterMapping.of(foo);
        var parameter = TgBindParameters.of(foo.bind(1));
        preparedStatement(sql, mapping, parameter);
    }

    private <P> void preparedStatement(String sql, TgParameterMapping<P> parameterMapping, P parameter) throws IOException, InterruptedException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql, parameterMapping)) {
            tm.execute(transaction -> {
                var e = assertThrowsExactly(TsurugiIOException.class, () -> {
                    transaction.executeAndGetCount(ps, parameter);
                });
                assertErrorTableNotFound(TEST, e);
            });
            tm.execute(transaction -> {
                var e = assertThrowsExactly(IceaxeIOException.class, () -> {
                    transaction.executeAndGetCount(ps, parameter);
                });
                assertEqualsCode(IceaxeErrorCode.PS_LOW_ERROR, e);
                assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e.getCause());
            });
        }
    }
}
