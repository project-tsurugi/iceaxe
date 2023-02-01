package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
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
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * Transaction error test
 */
class DbTransactionErrorTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void notCommitRollback() throws IOException, TsurugiTransactionException {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(123, 456, 'abc')";

        var session = getSession();
        try (var ps = session.createPreparedStatement(sql); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            int count = transaction.executeAndGetCount(ps);
            assertEquals(-1, count); // TODO 1

            // do not commit,rollback
        }

        // expected: auto rollback
        assertEqualsTestTable(SIZE);
    }

    @Test
    void writeToReadOnly() throws IOException {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(123, 456, 'abc')";

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofRTX());
        try (var ps = session.createPreparedStatement(sql)) {
            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> tm.executeAndGetCount(ps));
            assertEqualsCode(SqlServiceCode.ERR_UNSUPPORTED, e);
        }

        // expected: auto rollback
        assertEqualsTestTable(SIZE);
    }

    @Test
    void ltxWithoutWritePreserve() throws IOException {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(123, 456, 'abc')";

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofLTX()); // no WritePreserve
        try (var ps = session.createPreparedStatement(sql)) {
            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> tm.executeAndGetCount(ps));
            assertEqualsCode(SqlServiceCode.ERR_ILLEGAL_OPERATION, e);
        }

        // expected: auto rollback
        assertEqualsTestTable(SIZE);
    }

    @Test
    void limitOver() throws IOException {
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
    void commitAfterClose() throws IOException {
        var session = getSession();
        var transaction = session.createTransaction(TgTxOption.ofOCC());
        transaction.close();
        var e = assertThrowsExactly(TsurugiIOException.class, () -> transaction.commit(TgCommitType.DEFAULT));
        assertEqualsCode(IceaxeErrorCode.TX_ALREADY_CLOSED, e);
    }

    @Test
    void rollbackAfterClose() throws IOException {
        var session = getSession();
        var transaction = session.createTransaction(TgTxOption.ofOCC());
        transaction.close();
        var e = assertThrowsExactly(TsurugiIOException.class, () -> transaction.rollback());
        assertEqualsCode(IceaxeErrorCode.TX_ALREADY_CLOSED, e);
    }

    @Test
    void addChildAfterClose() throws IOException {
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
    void getLowAfterFutureClose() throws IOException {
        var session = getSession();
        var transaction = session.createTransaction(TgTxOption.ofOCC());
        transaction.close();
        var e = assertThrowsExactly(IOException.class, () -> {
            transaction.getLowTransaction();
        });
        assertMatches("Future .+ is already closed", e.getMessage());
    }

    @Test
    void getLowAfterClose() throws IOException {
        var session = getSession();
        var transaction = session.createTransaction(TgTxOption.ofOCC());
        var lowTx = transaction.getLowTransaction();
        transaction.close();
        assertSame(lowTx, transaction.getLowTransaction());
    }

    @Test
    void executeAfterClose() throws IOException {
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
