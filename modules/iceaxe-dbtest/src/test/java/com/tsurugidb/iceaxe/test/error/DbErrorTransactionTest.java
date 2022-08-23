package com.tsurugidb.iceaxe.test.error;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.nautilus_technologies.tsubakuro.exception.SqlServiceCode;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;

/**
 * Transaction error test
 */
class DbErrorTransactionTest extends DbTestTableTester {

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
            int count = ps.executeAndGetCount(transaction);
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
            var e = assertThrows(TsurugiTransactionIOException.class, () -> ps.executeAndGetCount(tm));
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
            var e = assertThrows(TsurugiTransactionIOException.class, () -> ps.executeAndGetCount(tm));
            assertEqualsCode(SqlServiceCode.ERR_ILLEGAL_OPERATION, e);
        }

        // expected: auto rollback
        assertEqualsTestTable(SIZE);
    }
}
