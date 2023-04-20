package com.tsurugidb.iceaxe.test.transaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * transaction execute statement test
 */
class DbTransactionExecuteStatementTest extends DbTestTableTester {

    private static final int SIZE = 2;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void executeStatement() throws Exception {
        var entity = createTestEntity(SIZE);
        var sql = "insert into " + TEST //
                + " (" + TEST_COLUMNS + ")" //
                + " values(" + entity.getFoo() + "," + entity.getBar() + ",'" + entity.getZzz() + "')";

        var session = getSession();
        try (var ps = session.createStatement(sql); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            try (var result = transaction.executeStatement(ps)) {
                assertUpdateCount(1, result.getUpdateCount());

                transaction.commit(TgCommitType.DEFAULT);
            }
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executePreparedStatement() throws Exception {
        var session = getSession();
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var entity = createTestEntity(SIZE);
            try (var result = transaction.executeStatement(ps, entity)) {
                assertUpdateCount(1, result.getUpdateCount());

                transaction.commit(TgCommitType.DEFAULT);
            }
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCount() throws Exception {
        var entity = createTestEntity(SIZE);
        var sql = "insert into " + TEST //
                + " (" + TEST_COLUMNS + ")" //
                + " values(" + entity.getFoo() + "," + entity.getBar() + ",'" + entity.getZzz() + "')";

        var session = getSession();
        try (var ps = session.createStatement(sql); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            int result = transaction.executeAndGetCount(ps);

            assertUpdateCount(1, result);

            transaction.commit(TgCommitType.DEFAULT);
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executePreparedAndGetCount() throws Exception {
        var session = getSession();
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var entity = createTestEntity(SIZE);
            int result = transaction.executeAndGetCount(ps, entity);

            assertUpdateCount(1, result);

            transaction.commit(TgCommitType.DEFAULT);
        }

        assertEqualsTestTable(SIZE + 1);
    }
}
