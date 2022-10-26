package com.tsurugidb.iceaxe.test.error;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
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
                var e0 = assertThrows(TsurugiTransactionIOException.class, () -> {
                    tm.execute((TsurugiTransactionAction) transaction -> {
                        var e = assertThrows(TsurugiTransactionException.class, () -> {
                            ps.executeAndGetList(transaction);
                        });
                        assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
                        throw e;
                    });
                });
                assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e0);
//              assertContains("table_not_found test.", e0.getMessage()); // TODO エラー詳細情報の確認
            }
        }
    }

    @Test
    void insert() throws IOException {
        var entity = new TestEntity(123, 456, "abc");

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                LOG.trace("i={}", i);
                var e0 = assertThrows(TsurugiIOException.class, () -> {
                    tm.execute((TsurugiTransactionAction) transaction -> {
                        var e = assertThrows(TsurugiIOException.class, () -> {
                            ps.executeAndGetCount(transaction, entity);
                        });
                        assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
                        throw e;
                    });
                });
                assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e0);
                assertContains("table_not_found table `test' is not found", e0.getMessage());
            }
        }
    }

    @Test
    void update() throws IOException {
        var sql = "update " + TEST + " set bar = 0";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                var e0 = assertThrows(TsurugiIOException.class, () -> {
                    tm.execute((TsurugiTransactionAction) transaction -> {
                        var e = assertThrows(TsurugiTransactionException.class, () -> {
                            ps.executeAndGetCount(transaction);
                        });
                        assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
                        throw e;
                    });
                });
                assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e0);
                assertContains("table_not_found test.", e0.getMessage());
            }
        }
    }

    @Test
    void delete() throws IOException {
        var sql = "delete from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                var e0 = assertThrows(TsurugiIOException.class, () -> {
                    tm.execute((TsurugiTransactionAction) transaction -> {
                        var e = assertThrows(TsurugiTransactionException.class, () -> {
                            ps.executeAndGetCount(transaction);
                        });
                        assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
                        throw e;
                    });
                });
                assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e0);
                assertContains("table_not_found test.", e0.getMessage());
            }
        }
    }
}
