package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmRetryOverIOException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * {@link TsurugiTmIOException} test
 */
class DbManagerExceptionTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbManagerExceptionTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(LOG, info);
    }

    @Test
    void exception() throws Exception {
        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            tm.execute(transaction -> {
                try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
                    var entity = createTestEntity(1);
                    var e0 = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                        transaction.executeAndGetCount(ps, entity);
                    });
                    assertEqualsCode(SqlServiceCode.UNIQUE_CONSTRAINT_VIOLATION_EXCEPTION, e0);
                    // not throw e0
                }
            });
        });
        assertEqualsCode(SqlServiceCode.INACTIVE_TRANSACTION_EXCEPTION, e);
        var status = e.getTransactionStatus();
        assertTrue(status.isError());
        assertEqualsCode(SqlServiceCode.UNIQUE_CONSTRAINT_VIOLATION_EXCEPTION, status.getTransactionException());
        var next = e.getNextTmOption();
        assertFalse(next.isExecute());
        assertFalse(next.isRetryOver());
    }

    @Test
    void retryException() throws Exception {
        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());
        var e = assertThrowsExactly(TsurugiTmRetryOverIOException.class, () -> {
            try (var ltx = session.createTransaction(TgTxOption.ofLTX(TEST))) {
                try {
                    ltx.getLowTransaction();
                    tm.execute((TsurugiTransactionAction) transaction -> {
                        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
                            var entity = createTestEntity(SIZE);
                            var e0 = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                                transaction.executeAndGetCount(ps, entity);
                            });
                            assertEqualsCode(SqlServiceCode.CONFLICT_ON_WRITE_PRESERVE_EXCEPTION, e0);
                            throw e0;
                        }
                    });
                } finally {
                    ltx.rollback();
                }
            }
        });
        assertEqualsCode(SqlServiceCode.CONFLICT_ON_WRITE_PRESERVE_EXCEPTION, e);
        var status = e.getTransactionStatus();
        assertTrue(status.isError());
        assertEqualsCode(SqlServiceCode.CONFLICT_ON_WRITE_PRESERVE_EXCEPTION, status.getTransactionException());
        var next = e.getNextTmOption();
        assertFalse(next.isExecute());
        assertTrue(next.isRetryOver());
    }
}
