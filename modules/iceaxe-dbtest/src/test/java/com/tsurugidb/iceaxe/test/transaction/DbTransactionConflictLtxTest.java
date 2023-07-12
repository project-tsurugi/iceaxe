package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * transaction conflict test
 */
class DbTransactionConflictLtxTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(info);
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
    private static final String DELETE_SQL = "delete from " + TEST + " where foo = " + (SIZE - 1);

    @Test
    void ltx_occR() throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL1, SELECT_MAPPING); //
                var updatePs = session.createStatement(UPDATE_SQL1)) {
            try (var tx1 = session.createTransaction(LTX)) {
                tx1.executeAndGetCount(updatePs);

                try (var tx2 = session.createTransaction(OCC)) {
                    var e = assertThrows(TsurugiTransactionException.class, () -> {
                        tx2.executeAndFindRecord(selectPs).get();
                    });
                    assertEqualsCode(SqlServiceCode.ERR_SERIALIZATION_FAILURE, e);

                    tx2.rollback();
                }

                var entity12 = tx1.executeAndFindRecord(selectPs).get();
                assertEquals(BAR_AFTER1, entity12.getBar());

                tx1.commit(TgCommitType.DEFAULT);
            }
        }
    }

    @Test
    void ltx_rtx() throws Exception {
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
    void ltx_occW() throws Exception {
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
                    assertEqualsCode(SqlServiceCode.ERR_SERIALIZATION_FAILURE, e);

                    tx2.rollback();
                }

                var entity12 = tx1.executeAndFindRecord(selectPs).get();
                assertEquals(BAR_AFTER1, entity12.getBar());

                tx1.commit(TgCommitType.DEFAULT);
            }
        }
    }

    @Test
    void ltx_ltx() throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL1, SELECT_MAPPING); //
                var updatePs = session.createStatement(UPDATE_SQL1); //
                var updatePs2 = session.createStatement(UPDATE_SQL2)) {
            try (var tx1 = session.createTransaction(LTX)) {
                tx1.executeAndGetCount(updatePs);

                try (var tx2 = session.createTransaction(LTX)) {
                    tx2.executeAndGetCount(updatePs2);

                    var future2 = executeFuture(() -> {
                        tx2.commit(TgCommitType.DEFAULT);
                        return null;
                    });

                    var entity12 = tx1.executeAndFindRecord(selectPs).get();
                    assertEquals(BAR_AFTER1, entity12.getBar());

                    assertFalse(future2.isDone());
                    tx1.commit(TgCommitType.DEFAULT);

                    var e = assertThrows(TsurugiTransactionException.class, () -> {
                        try {
                            future2.get();
                        } catch (ExecutionException ee) {
                            throw ee.getCause();
                        }
                    });
                    assertEqualsCode(SqlServiceCode.ERR_SERIALIZATION_FAILURE, e);
                }
            }
        }
    }

    @Test
    void ltx_ltx2() throws Exception {
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
                    assertEqualsCode(SqlServiceCode.ERR_SERIALIZATION_FAILURE, e);
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, -1 })
    void ltx_occW_phantom(int add) throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL, SELECT_MAPPING); //
                var insertPs = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                var deletePs = session.createStatement(DELETE_SQL)) {
            try (var tx1 = session.createTransaction(LTX)) {
                var list11 = tx1.executeAndGetList(selectPs);
                assertEquals(SIZE, list11.size());

                try (var tx2 = session.createTransaction(OCC)) {
                    var e = assertThrows(TsurugiTransactionException.class, () -> {
                        if (add > 0) {
                            var entity2 = createTestEntity(SIZE);
                            tx2.executeAndGetCount(insertPs, entity2);
                        } else {
                            tx2.executeAndGetCount(deletePs);
                        }
                    });
//TODO              assertEqualsCode(SqlServiceCode.ERR_SERIALIZATION_FAILURE, e);
                    var code = e.getDiagnosticCode();
                    assertTrue(code == SqlServiceCode.ERR_SERIALIZATION_FAILURE || code == SqlServiceCode.ERR_CONFLICT_ON_WRITE_PRESERVE);

                    tx2.rollback();
                }

                var list12 = tx1.executeAndGetList(selectPs);
                assertEquals(SIZE, list12.size());

                tx1.commit(TgCommitType.DEFAULT);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, -1 })
    void ltx_ltx_phantom(int add) throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL, SELECT_MAPPING); //
                var insertPs = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                var deletePs = session.createStatement(DELETE_SQL)) {
            try (var tx1 = session.createTransaction(LTX)) {
                var list11 = tx1.executeAndGetList(selectPs);
                assertEquals(SIZE, list11.size());

                try (var tx2 = session.createTransaction(LTX)) {
                    if (add > 0) {
                        var entity2 = createTestEntity(SIZE);
                        tx2.executeAndGetCount(insertPs, entity2);
                    } else {
                        tx2.executeAndGetCount(deletePs);
                    }

                    var future2 = executeFuture(() -> {
                        tx2.commit(TgCommitType.DEFAULT);
                        return null;
                    });

                    var list12 = tx1.executeAndGetList(selectPs);
                    assertEquals(SIZE, list12.size());

                    assertFalse(future2.isDone());
                    tx1.commit(TgCommitType.DEFAULT);

                    future2.get();
                }
            }
        }

        assertEqualsTestTable(SIZE + add);
    }
}
