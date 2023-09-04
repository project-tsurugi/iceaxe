package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
class DbTransactionConflictOccTest extends DbTestTableTester {

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
    void occR_occR() throws Exception {
        occR_r(OCC);
    }

    @Test
    void occR_rtx() throws Exception {
        occR_r(RTX);
    }

    private void occR_r(TgTxOption txOption2) throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL1, SELECT_MAPPING)) {
            try (var tx1 = session.createTransaction(OCC)) {
                var entity11 = tx1.executeAndFindRecord(selectPs).get();
                assertEquals(BAR_BEFORE, entity11.getBar());

                try (var tx2 = session.createTransaction(txOption2)) {
                    var entity2 = tx2.executeAndFindRecord(selectPs).get();
                    assertEquals(BAR_BEFORE, entity2.getBar());

                    tx2.commit(TgCommitType.DEFAULT);
                }

                var entity12 = tx1.executeAndFindRecord(selectPs).get();
                assertEquals(BAR_BEFORE, entity12.getBar());

                tx1.commit(TgCommitType.DEFAULT);
            }
        }
    }

    @Test
    void occR_occW() throws Exception {
        occR_w(OCC);
    }

    @Test
    void occR_ltx() throws Exception {
        occR_w(LTX);
    }

    private void occR_w(TgTxOption txOption2) throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL1, SELECT_MAPPING); //
                var updatePs = session.createStatement(UPDATE_SQL1)) {
            try (var tx1 = session.createTransaction(OCC)) {
                var entity11 = tx1.executeAndFindRecord(selectPs).get();
                assertEquals(BAR_BEFORE, entity11.getBar());

                try (var tx2 = session.createTransaction(txOption2)) {
                    tx2.executeAndGetCount(updatePs);

                    tx2.commit(TgCommitType.DEFAULT);
                }

                var entity12 = tx1.executeAndFindRecord(selectPs).get();
                assertEquals(BAR_AFTER1, entity12.getBar());

                var e = assertThrows(TsurugiTransactionException.class, () -> {
                    tx1.commit(TgCommitType.DEFAULT);
                });
                assertEqualsCode(SqlServiceCode.CC_EXCEPTION, e);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, -1 })
    void occR_occW_phantom(int add) throws Exception {
        occR_w_phantom(OCC, add);
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, -1 })
    void occR_ltx_phantom(int add) throws Exception {
        occR_w_phantom(LTX, add);
    }

    private void occR_w_phantom(TgTxOption txOption2, int add) throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL, SELECT_MAPPING); //
                var insertPs = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                var deletePs = session.createStatement(DELETE_SQL)) {
            try (var tx1 = session.createTransaction(OCC)) {
                var list11 = tx1.executeAndGetList(selectPs);
                assertEquals(SIZE, list11.size());

                try (var tx2 = session.createTransaction(txOption2)) {
                    if (add > 0) {
                        var entity2 = createTestEntity(SIZE);
                        tx2.executeAndGetCount(insertPs, entity2);
                    } else {
                        tx2.executeAndGetCount(deletePs);
                    }

                    tx2.commit(TgCommitType.DEFAULT);
                }

                var list12 = tx1.executeAndGetList(selectPs);
                assertEquals(SIZE + add, list12.size());

                var e = assertThrows(TsurugiTransactionException.class, () -> {
                    tx1.commit(TgCommitType.DEFAULT);
                });
                assertEqualsCode(SqlServiceCode.CC_EXCEPTION, e);
            }
        }

        assertEqualsTestTable(SIZE + add);
    }

    @Test
    void occW_occR() throws Exception {
        occW_r(OCC);
    }

    @Test
    void occW_rtx() throws Exception {
        occW_r(RTX);
    }

    private void occW_r(TgTxOption txOption2) throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL1, SELECT_MAPPING); //
                var updatePs = session.createStatement(UPDATE_SQL1)) {
            try (var tx1 = session.createTransaction(OCC)) {
                tx1.executeAndGetCount(updatePs);

                try (var tx2 = session.createTransaction(txOption2)) {
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
    void occW_occW() throws Exception {
        occW_w(OCC);
    }

    @Test
    void occW_ltx() throws Exception {
        occW_w(LTX);
    }

    private void occW_w(TgTxOption txOption2) throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL1, SELECT_MAPPING); //
                var updatePs = session.createStatement(UPDATE_SQL1); //
                var updatePs2 = session.createStatement(UPDATE_SQL2)) {
            try (var tx1 = session.createTransaction(OCC)) {
                tx1.executeAndGetCount(updatePs);

                try (var tx2 = session.createTransaction(txOption2)) {
                    tx2.executeAndGetCount(updatePs2);

                    tx2.commit(TgCommitType.DEFAULT);
                }

                var entity12 = tx1.executeAndFindRecord(selectPs).get();
                assertEquals(BAR_AFTER1, entity12.getBar());

                var e = assertThrows(TsurugiTransactionException.class, () -> {
                    tx1.commit(TgCommitType.DEFAULT);
                });
                assertEqualsCode(SqlServiceCode.CC_EXCEPTION, e);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3 })
    void occW_ltx2(int position) throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL1, SELECT_MAPPING); //
                var updatePs = session.createStatement(UPDATE_SQL1); //
                var updatePs2 = session.createStatement(UPDATE_SQL2)) {
            try (var tx1 = session.createTransaction(OCC)) {
                var entity11 = tx1.executeAndFindRecord(selectPs).get();
                long expected = BAR_BEFORE;
                assertEquals(expected, entity11.getBar());

                if (position == 1) {
                    tx1.executeAndGetCount(updatePs);
                    expected = BAR_AFTER1;
                }
                try (var tx2 = session.createTransaction(LTX)) {
                    var entity2 = tx2.executeAndFindRecord(selectPs).get();
                    assertEquals(BAR_BEFORE, entity2.getBar());

                    if (position == 2) {
                        var e = assertThrows(TsurugiTransactionException.class, () -> {
                            tx1.executeAndGetCount(updatePs);
                        });
                        assertEqualsCode(SqlServiceCode.CC_EXCEPTION, e);
                        tx1.rollback();
                    }

                    tx2.executeAndGetCount(updatePs2);

                    tx2.commit(TgCommitType.DEFAULT);

                    if (position == 2) {
                        return;
                    }
                    if (position != 1) {
                        expected = BAR_AFTER2;
                    }
                }
                if (position == 3) {
                    tx1.executeAndGetCount(updatePs);
                    expected = BAR_AFTER1;
                }

                var entity12 = tx1.executeAndFindRecord(selectPs).get();
                assertEquals(expected, entity12.getBar());

                var e = assertThrows(TsurugiTransactionException.class, () -> {
                    tx1.commit(TgCommitType.DEFAULT);
                });
                assertEqualsCode(SqlServiceCode.CC_EXCEPTION, e);
            }
        }
    }

    @Test
    void occW_occW3() throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL1, SELECT_MAPPING); //
                var updatePs = session.createStatement(UPDATE_SQL1); //
                var updatePs2 = session.createStatement(UPDATE_SQL2)) {
            try (var tx1 = session.createTransaction(OCC)) {
                tx1.executeAndGetCount(updatePs);

                try (var tx2 = session.createTransaction(OCC)) {
                    tx2.executeAndGetCount(updatePs2);

                    var entity12 = tx1.executeAndFindRecord(selectPs).get();
                    assertEquals(BAR_AFTER1, entity12.getBar());

                    tx1.commit(TgCommitType.DEFAULT);

                    var e = assertThrows(TsurugiTransactionException.class, () -> {
                        tx2.commit(TgCommitType.DEFAULT);
                    });
                    assertEqualsCode(SqlServiceCode.CC_EXCEPTION, e);
                }
            }
        }
    }

    @Test
    void occW_ltx3() throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL1, SELECT_MAPPING); //
                var updatePs = session.createStatement(UPDATE_SQL1); //
                var updatePs2 = session.createStatement(UPDATE_SQL2)) {
            try (var tx1 = session.createTransaction(OCC)) {
                tx1.executeAndGetCount(updatePs);

                try (var tx2 = session.createTransaction(LTX)) {
                    tx2.executeAndGetCount(updatePs2);

                    var e = assertThrows(TsurugiTransactionException.class, () -> {
                        tx1.executeAndFindRecord(selectPs).get();
                    });
                    assertEqualsCode(SqlServiceCode.CC_EXCEPTION, e);
                    tx1.rollback();

                    tx2.commit(TgCommitType.DEFAULT);
                }
            }
        }
    }
}
