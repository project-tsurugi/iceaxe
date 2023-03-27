package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * transaction conflict test
 */
class DbTransactionConflictLtxTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("{} init end", info.getDisplayName());
    }

    private static final TgTxOption OCC = TgTxOption.ofOCC();
    private static final TgTxOption LTX = TgTxOption.ofLTX(TEST);
    private static final TgTxOption RTX = TgTxOption.ofRTX();

    private static final int KEY = 1;
    private static final long BAR_BEFORE = 1;
    private static final long BAR_AFTER1 = 789;
    private static final long BAR_AFTER2 = 999;
    private static final String SELECT_SQL1 = SELECT_SQL + " where foo = " + KEY;
    private static final String UPDATE_SQL1 = "update " + TEST + " set bar =  " + BAR_AFTER1 + " where foo = " + KEY;
    private static final String UPDATE_SQL2 = "update " + TEST + " set bar =  " + BAR_AFTER2 + " where foo = " + KEY;

    @Test
    void ltx_occR() throws IOException, TsurugiTransactionException {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL1, SELECT_MAPPING); //
                var updatePs = session.createStatement(UPDATE_SQL1)) {
            try (var tx1 = session.createTransaction(LTX)) {
                tx1.executeAndGetCount(updatePs);

                try (var tx2 = session.createTransaction(OCC)) {
                    var e = assertThrows(TsurugiTransactionException.class, () -> {
                        tx2.executeAndFindRecord(selectPs).get();
                    });
                    assertEqualsCode(SqlServiceCode.ERR_ABORTED_RETRYABLE, e);

                    tx2.rollback();
                }

                var entity12 = tx1.executeAndFindRecord(selectPs).get();
                assertEquals(BAR_AFTER1, entity12.getBar());

                tx1.commit(TgCommitType.DEFAULT);
            }
        }
    }

    @Test
    void ltx_rtx() throws IOException, TsurugiTransactionException {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL1, SELECT_MAPPING); //
                var updatePs = session.createStatement(UPDATE_SQL1)) {
            try (var tx1 = session.createTransaction(LTX)) {
                tx1.executeAndGetCount(updatePs);

                try (var tx2 = session.createTransaction(RTX)) {
                    var entity2 = tx2.executeAndFindRecord(selectPs).get();
                    assertEquals(BAR_BEFORE, entity2.getBar());

                    tx2.commit(TgCommitType.DEFAULT);
                }

                var entity12 = tx1.executeAndFindRecord(selectPs).get();
                assertEquals(BAR_AFTER1, entity12.getBar());

                tx1.commit(TgCommitType.DEFAULT);
            }
        }
    }

    @Test
    void ltx_occW() throws IOException, TsurugiTransactionException {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL1, SELECT_MAPPING); //
                var updatePs = session.createStatement(UPDATE_SQL1); //
                var updatePs2 = session.createStatement(UPDATE_SQL2)) {
            try (var tx1 = session.createTransaction(LTX)) {
                tx1.executeAndGetCount(updatePs);

                try (var tx2 = session.createTransaction(OCC)) {
                    var e = assertThrows(TsurugiTransactionException.class, () -> {
                        tx2.executeAndGetCount(updatePs2);
                    });
                    assertEqualsCode(SqlServiceCode.ERR_ABORTED_RETRYABLE, e);

                    tx2.rollback();
                }

                var entity12 = tx1.executeAndFindRecord(selectPs).get();
                assertEquals(BAR_AFTER1, entity12.getBar());

                tx1.commit(TgCommitType.DEFAULT);
            }
        }
    }

    @Test
    void ltx_ltx() throws IOException, TsurugiTransactionException {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL1, SELECT_MAPPING); //
                var updatePs = session.createStatement(UPDATE_SQL1); //
                var updatePs2 = session.createStatement(UPDATE_SQL2)) {
            try (var tx1 = session.createTransaction(LTX)) {
                tx1.executeAndGetCount(updatePs);

                try (var tx2 = session.createTransaction(LTX)) {
                    tx2.executeAndGetCount(updatePs2);

                    var future2 = Executors.newFixedThreadPool(1).submit(() -> {
                        try {
                            tx2.commit(TgCommitType.DEFAULT);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e.getMessage(), e);
                        } catch (TsurugiTransactionException e) {
                            throw new TsurugiTransactionRuntimeException(e);
                        }
                    });

                    var entity12 = tx1.executeAndFindRecord(selectPs).get();
                    assertEquals(BAR_AFTER1, entity12.getBar());

                    tx1.commit(TgCommitType.DEFAULT);

                    var e = assertThrows(TsurugiTransactionRuntimeException.class, () -> {
                        try {
                            future2.get();
                        } catch (ExecutionException ee) {
                            throw ee.getCause();
                        }
                    });
                    assertEqualsCode(SqlServiceCode.ERR_ABORTED_RETRYABLE, e);
                }
            }
        }
    }

    @Test
    void ltx_ltx2() throws IOException, TsurugiTransactionException {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL1, SELECT_MAPPING); //
                var updatePs = session.createStatement(UPDATE_SQL1); //
                var updatePs2 = session.createStatement(UPDATE_SQL2)) {
            try (var tx1 = session.createTransaction(LTX)) {
                tx1.executeAndGetCount(updatePs);

                try (var tx2 = session.createTransaction(LTX)) {
                    tx2.executeAndGetCount(updatePs2);

                    var entity12 = tx1.executeAndFindRecord(selectPs).get();
                    assertEquals(BAR_AFTER1, entity12.getBar());

                    tx1.commit(TgCommitType.DEFAULT);

                    var e = assertThrows(TsurugiTransactionException.class, () -> {
                        tx2.commit(TgCommitType.DEFAULT);
                    });
                    assertEqualsCode(SqlServiceCode.ERR_ABORTED_RETRYABLE, e);
                }
            }
        }
    }
}
