package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * transaction execute statement test
 */
class DbTransactionExecuteStatementTest extends DbTestTableTester {

    private static final int SIZE = 2;

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void executeStatement() throws IOException, TsurugiTransactionException {
        var entity = createTestEntity(SIZE);
        var sql = "insert into " + TEST //
                + " (" + TEST_COLUMNS + ")" //
                + " values(" + entity.getFoo() + "," + entity.getBar() + ",'" + entity.getZzz() + "')";

        var session = getSession();
        try (var ps = session.createPreparedStatement(sql); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            try (var rc = transaction.executeStatement(ps)) {
                assertEquals(-1, rc.getUpdateCount()); // TODO 1

                transaction.commit(TgCommitType.DEFAULT);
            }
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executePreparedStatement() throws IOException, TsurugiTransactionException {
        var session = getSession();
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var entity = createTestEntity(SIZE);
            try (var rc = transaction.executeStatement(ps, entity)) {
                assertEquals(-1, rc.getUpdateCount()); // TODO 1

                transaction.commit(TgCommitType.DEFAULT);
            }
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCount() throws IOException, TsurugiTransactionException {
        var entity = createTestEntity(SIZE);
        var sql = "insert into " + TEST //
                + " (" + TEST_COLUMNS + ")" //
                + " values(" + entity.getFoo() + "," + entity.getBar() + ",'" + entity.getZzz() + "')";

        var session = getSession();
        try (var ps = session.createPreparedStatement(sql); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            int result = transaction.executeAndGetCount(ps);

            assertEquals(-1, result); // TODO 1

            transaction.commit(TgCommitType.DEFAULT);
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executePreparedAndGetCount() throws IOException, TsurugiTransactionException {
        var session = getSession();
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var entity = createTestEntity(SIZE);
            int result = transaction.executeAndGetCount(ps, entity);

            assertEquals(-1, result); // TODO 1

            transaction.commit(TgCommitType.DEFAULT);
        }

        assertEqualsTestTable(SIZE + 1);
    }
}