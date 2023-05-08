package com.tsurugidb.iceaxe.test.transaction;

import java.util.ArrayList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * transaction execute statement test
 */
class DbTransactionExecuteStatementTest extends DbTestTableTester {

    private static final int SIZE = 2;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(info);
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
    @Disabled // TODO remove Disabled. Waiting for implementation in jogasaki
    void executeBatch() throws Exception {
        int addSize = 4;

        var session = getSession();
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var entityList = new ArrayList<TestEntity>(addSize);
            for (int i = 0; i < addSize; i++) {
                var entity = createTestEntity(SIZE + i);
                entityList.add(entity);
            }
            try (var result = transaction.executeBatch(ps, entityList)) {
                assertUpdateCount(addSize, result.getUpdateCount());

                transaction.commit(TgCommitType.DEFAULT);
            }
        }

        assertEqualsTestTable(SIZE + addSize);
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

    @Test
    @Disabled // TODO remove Disabled. Waiting for implementation in jogasaki
    void executeBatchAndGetCount() throws Exception {
        int addSize = 4;

        var session = getSession();
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var entityList = new ArrayList<TestEntity>(addSize);
            for (int i = 0; i < addSize; i++) {
                var entity = createTestEntity(SIZE + i);
                entityList.add(entity);
            }
            int result = transaction.executeAndGetCount(ps, entityList);

            assertUpdateCount(addSize, result);

            transaction.commit(TgCommitType.DEFAULT);
        }

        assertEqualsTestTable(SIZE + addSize);
    }
}
