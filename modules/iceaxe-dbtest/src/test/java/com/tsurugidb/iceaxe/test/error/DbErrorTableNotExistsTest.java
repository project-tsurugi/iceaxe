package com.tsurugidb.iceaxe.test.error;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * table not exists test
 */
class DbErrorTableNotExistsTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll() throws IOException {
        var LOG = LoggerFactory.getLogger(DbErrorTableNotExistsTest.class);
        LOG.debug("init start");

        dropTestTable();

        LOG.debug("init end");
    }

    @Test
    @Disabled // TODO change Disabled to Timeout
    void select() throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(SELECT_SQL)) {
            for (int i = 0; i < 30; i++) {
                var e0 = assertThrows(TsurugiTransactionIOException.class, () -> {
                    tm.execute((TsurugiTransactionAction) transaction -> {
                        var e = assertThrows(TsurugiTransactionException.class, () -> {
                            ps.executeAndGetList(transaction);
                        });
                        assertEqualsCode(SqlServiceCode.ERR_TRANSLATOR_ERROR, e); // TODO table_not_found
                        throw e;
                    });
                });
                assertEqualsCode(SqlServiceCode.ERR_TRANSLATOR_ERROR, e0); // TODO table_not_found
            }
        }
    }

    @Test
    void insert() throws IOException {
        var entity = new TestEntity(123, 456, "abc");

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            for (int i = 0; i < 30; i++) {
                LOG.trace("i={}", i);
                var e0 = assertThrows(TsurugiIOException.class, () -> {
                    tm.execute((TsurugiTransactionAction) transaction -> {
                        var e = assertThrows(TsurugiIOException.class, () -> {
                            ps.executeAndGetCount(transaction, entity);
                        });
                        assertEqualsCode(SqlServiceCode.ERR_TRANSLATOR_ERROR, e); // TODO table_not_found
                        throw e;
                    });
                });
                assertEqualsCode(SqlServiceCode.ERR_TRANSLATOR_ERROR, e0); // TODO table_not_found
            }
        }
    }

    @Test
    void update() throws IOException {
        var sql = "update " + TEST + " set bar = 0";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            var e0 = assertThrows(TsurugiIOException.class, () -> {
                tm.execute((TsurugiTransactionAction) transaction -> {
                    var e = assertThrows(TsurugiTransactionException.class, () -> {
                        ps.executeAndGetCount(transaction);
                    });
                    assertEqualsCode(SqlServiceCode.ERR_TRANSLATOR_ERROR, e); // TODO table_not_found
                    throw e;
                });
            });
            assertEqualsCode(SqlServiceCode.ERR_TRANSLATOR_ERROR, e0); // TODO table_not_found
        }
    }

    @Test
    void delete() throws IOException {
        var sql = "delete from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            var e0 = assertThrows(TsurugiIOException.class, () -> {
                tm.execute((TsurugiTransactionAction) transaction -> {
                    var e = assertThrows(TsurugiTransactionException.class, () -> {
                        ps.executeAndGetCount(transaction);
                    });
                    assertEqualsCode(SqlServiceCode.ERR_TRANSLATOR_ERROR, e); // TODO table_not_found
                    throw e;
                });
            });
            assertEqualsCode(SqlServiceCode.ERR_TRANSLATOR_ERROR, e0); // TODO table_not_found
        }
    }
}
