package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgCommitOption;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.TransactionStatus;

/**
 * transaction commit aut-dispose test
 */
class DbTransactionCommitAutoDisposeTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbTransactionCommitAutoDisposeTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();

        logInitEnd(LOG, info);
    }

    @Test
    void autoDispose_false() throws Exception {
        var session = getSession();
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            {
                var status = transaction.getTransactionStatus();
                assertTrue(status.isTransactionFound());
                assertEquals(TransactionStatus.RUNNING, status.getLowTransactionStatus());
            }

            var commitOption = TgCommitOption.of().autoDispose(false);
            transaction.commit(commitOption);

            // after commit, before close
            {
                var status = transaction.getTransactionStatus();
                assertTrue(status.isTransactionFound());
                assertEquals(TransactionStatus.STORED, status.getLowTransactionStatus());
            }
        }
    }

    @Test
    void autoDispose_true() throws Exception {
        var session = getSession();
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            {
                var status = transaction.getTransactionStatus();
                assertTrue(status.isTransactionFound());
                assertEquals(TransactionStatus.RUNNING, status.getLowTransactionStatus());
            }

            var commitOption = TgCommitOption.of().autoDispose(true);
            transaction.commit(commitOption);

            // after commit, before close
            {
                var status = transaction.getTransactionStatus();
                assertFalse(status.isTransactionFound());
                assertNull(status.getLowTransactionStatus());
            }
        }
    }
}
