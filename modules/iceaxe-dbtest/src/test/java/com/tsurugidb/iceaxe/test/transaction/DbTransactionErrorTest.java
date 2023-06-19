package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * Transaction error test
 */
class DbTransactionErrorTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(info);
    }

    @Test
    void notCommitRollback() throws Exception {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(123, 456, 'abc')";

        var session = getSession();
        try (var ps = session.createStatement(sql); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            int count = transaction.executeAndGetCount(ps);
            assertUpdateCount(1, count);

            // do not commit,rollback
        }

        // expected: auto rollback
        assertEqualsTestTable(SIZE);
    }

    @Test
    void writeToReadOnly() throws Exception {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(123, 456, 'abc')";

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofRTX());
        try (var ps = session.createStatement(sql)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> tm.executeAndGetCount(ps));
            assertEqualsCode(SqlServiceCode.ERR_UNSUPPORTED, e);
        }

        // expected: auto rollback
        assertEqualsTestTable(SIZE);
    }

    @Test
    void ltxWithoutWritePreserve() throws Exception {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(123, 456, 'abc')";

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofLTX()); // no WritePreserve
        try (var ps = session.createStatement(sql)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> tm.executeAndGetCount(ps));
            assertEqualsCode(SqlServiceCode.ERR_ILLEGAL_OPERATION, e);
        }

        // expected: auto rollback
        assertEqualsTestTable(SIZE);
    }

    @Test
    void limitOver() throws Exception {
        try (var session = DbTestConnector.createSession()) {
            for (int i = 0; i < 1000; i++) {
                LOG.trace("i={}", i);
                var tx = session.createTransaction(TgTxOption.ofOCC());
                try {
                    tx.getLowTransaction();
                } catch (TsurugiIOException e) {
                    assertEqualsCode(SqlServiceCode.ERR_RESOURCE_LIMIT_REACHED, e);
                    assertContains("creating transaction failed with error:err_resource_limit_reached", e.getMessage());
                    return;
                }
            }
            fail("err_resource_limit_reached did not occur");
        }
    }

    @Test
    void executeAfterCommit() throws Exception {
        var session = getSession();
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {

            transaction.commit(TgCommitType.DEFAULT);

            try (var ps = session.createQuery(SELECT_SQL)) {
                var e = assertThrows(IOException.class, () -> transaction.executeAndGetList(ps));
                assertEquals("transaction already closed", e.getMessage());
            }
        }
    }

    @Test
    void executeAfterRollback() throws Exception {
        var session = getSession();
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {

            transaction.rollback();

            try (var ps = session.createQuery(SELECT_SQL)) {
                var e = assertThrows(IOException.class, () -> transaction.executeAndGetList(ps));
                assertEquals("transaction already closed", e.getMessage());
            }
        }
    }

    @Test
    void executeAfterError() throws Exception {
        var session = getSession();
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            try (var insertPs = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                    var selectPs = session.createQuery(SELECT_SQL)) {
                var entity = createTestEntity(1);
                var e1 = assertThrowsExactly(TsurugiTransactionException.class, () -> transaction.executeAndGetCount(insertPs, entity));
                assertEqualsCode(SqlServiceCode.ERR_UNIQUE_CONSTRAINT_VIOLATION, e1);

                var e2 = assertThrowsExactly(TsurugiTransactionException.class, () -> transaction.executeAndGetList(selectPs));
                assertEqualsCode(SqlServiceCode.ERR_INACTIVE_TRANSACTION, e2);
            }
        }
    }

    @Test
    void commitAfterClose() throws Exception {
        var session = getSession();
        var transaction = session.createTransaction(TgTxOption.ofOCC());
        transaction.close();
        var e = assertThrowsExactly(TsurugiIOException.class, () -> transaction.commit(TgCommitType.DEFAULT));
        assertEqualsCode(IceaxeErrorCode.TX_ALREADY_CLOSED, e);
    }

    @Test
    void rollbackAfterClose() throws Exception {
        var session = getSession();
        var transaction = session.createTransaction(TgTxOption.ofOCC());
        transaction.close();
        var e = assertThrowsExactly(TsurugiIOException.class, () -> transaction.rollback());
        assertEqualsCode(IceaxeErrorCode.TX_ALREADY_CLOSED, e);
    }

    @Test
    void addChildAfterClose() throws Exception {
        var session = getSession();
        var transaction = session.createTransaction(TgTxOption.ofOCC());
        transaction.close();
        var e = assertThrowsExactly(TsurugiIOException.class, () -> {
            transaction.addChild(() -> {
                // dummy
            });
        });
        assertEqualsCode(IceaxeErrorCode.TX_ALREADY_CLOSED, e);
    }

    @Test
    void getLowAfterFutureClose() throws Exception {
        var session = getSession();
        var transaction = session.createTransaction(TgTxOption.ofOCC());
        transaction.close();
        var e = assertThrowsExactly(IOException.class, () -> {
            transaction.getLowTransaction();
        });
        assertMatches("Future .+ is already closed", e.getMessage());
    }

    @Test
    void getLowAfterClose() throws Exception {
        var session = getSession();
        var transaction = session.createTransaction(TgTxOption.ofOCC());
        var lowTx = transaction.getLowTransaction();
        transaction.close();
        assertSame(lowTx, transaction.getLowTransaction());
    }

    @Test
    void executeAfterClose() throws Exception {
        var session = getSession();
        var transaction = session.createTransaction(TgTxOption.ofOCC());
        transaction.close();
        var e = assertThrowsExactly(TsurugiIOException.class, () -> {
            transaction.executeLow(lowTx -> {
                return null; // dummy
            });
        });
        assertEqualsCode(IceaxeErrorCode.TX_ALREADY_CLOSED, e);
    }
}
