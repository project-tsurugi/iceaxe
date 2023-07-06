package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * transaction conflict test
 */
class DbTransactionConflictRtxTest extends DbTestTableTester {

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
    private static final long BAR_AFTER2 = 999;
    private static final String SELECT_SQL1 = SELECT_SQL + " where foo = " + KEY;
    private static final String UPDATE_SQL2 = "update " + TEST + " set bar =  " + BAR_AFTER2 + " where foo = " + KEY;
    private static final String DELETE_SQL = "delete from " + TEST + " where foo = " + (SIZE - 1);

    @Test
    void rtx_occR() throws Exception {
        rtx_r(OCC);
    }

    @Test
    void rtx_rtx() throws Exception {
        rtx_r(RTX);
    }

    private void rtx_r(TgTxOption txOption2) throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL1, SELECT_MAPPING)) {
            try (var tx1 = session.createTransaction(RTX)) {
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
    void rtx_occW() throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL1, SELECT_MAPPING); //
                var updatePs2 = session.createStatement(UPDATE_SQL2)) {
            try (var tx1 = session.createTransaction(RTX)) {
                var entity11 = tx1.executeAndFindRecord(selectPs).get();
                assertEquals(BAR_BEFORE, entity11.getBar());

                try (var tx2 = session.createTransaction(OCC)) {
                    tx2.executeAndGetCount(updatePs2);

                    tx2.commit(TgCommitType.DEFAULT);
                }

                var entity12 = tx1.executeAndFindRecord(selectPs).get();
                assertEquals(BAR_BEFORE, entity12.getBar());

                tx1.commit(TgCommitType.DEFAULT);
            }
        }
    }

    @Test
    void rtx_ltx() throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL1, SELECT_MAPPING); //
                var updatePs2 = session.createStatement(UPDATE_SQL2)) {
            try (var tx1 = session.createTransaction(RTX)) {
                var entity11 = tx1.executeAndFindRecord(selectPs).get();
                assertEquals(BAR_BEFORE, entity11.getBar());

                try (var tx2 = session.createTransaction(LTX)) {
                    tx2.executeAndGetCount(updatePs2);

                    tx2.commit(TgCommitType.DEFAULT);

                    var entity12 = tx1.executeAndFindRecord(selectPs).get();
                    assertEquals(BAR_BEFORE, entity12.getBar());

                    tx1.commit(TgCommitType.DEFAULT);
                }
            }
        }
    }

    @Test
    void rtx_ltx2() throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL1, SELECT_MAPPING); //
                var updatePs2 = session.createStatement(UPDATE_SQL2)) {
            try (var tx1 = session.createTransaction(LTX)) {
                var entity11 = tx1.executeAndFindRecord(selectPs).get();
                assertEquals(BAR_BEFORE, entity11.getBar());

                try (var tx2 = session.createTransaction(LTX)) {
                    tx2.executeAndGetCount(updatePs2);

                    var entity12 = tx1.executeAndFindRecord(selectPs).get();
                    assertEquals(BAR_BEFORE, entity12.getBar());

                    tx1.commit(TgCommitType.DEFAULT);

                    tx2.commit(TgCommitType.DEFAULT);
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, -1 })
    void rtx_occW_phantom(int add) throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL, SELECT_MAPPING); //
                var insertPs = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                var deletePs = session.createStatement(DELETE_SQL)) {
            try (var tx1 = session.createTransaction(RTX)) {
                var list11 = tx1.executeAndGetList(selectPs);
                assertEquals(SIZE, list11.size());

                try (var tx2 = session.createTransaction(OCC)) {
                    if (add > 0) {
                        var entity2 = createTestEntity(SIZE);
                        tx2.executeAndGetCount(insertPs, entity2);
                    } else {
                        tx2.executeAndGetCount(deletePs);
                    }

                    tx2.commit(TgCommitType.DEFAULT);
                }

                var list12 = tx1.executeAndGetList(selectPs);
                assertEquals(SIZE, list12.size());

                tx1.commit(TgCommitType.DEFAULT);
            }
        }

        assertEqualsTestTable(SIZE + add);
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, -1 })
    void rtx_ltx_phantom(int add) throws Exception {
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL, SELECT_MAPPING); //
                var insertPs = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                var deletePs = session.createStatement(DELETE_SQL)) {
            try (var tx1 = session.createTransaction(RTX)) {
                var list11 = tx1.executeAndGetList(selectPs);
                assertEquals(SIZE, list11.size());

                try (var tx2 = session.createTransaction(LTX)) {
                    if (add > 0) {
                        var entity2 = createTestEntity(SIZE);
                        tx2.executeAndGetCount(insertPs, entity2);
                    } else {
                        tx2.executeAndGetCount(deletePs);
                    }

                    tx2.commit(TgCommitType.DEFAULT);

                    var list12 = tx1.executeAndGetList(selectPs);
                    assertEquals(SIZE, list12.size());

                    tx1.commit(TgCommitType.DEFAULT);
                }
            }
        }

        assertEqualsTestTable(SIZE + add);
    }
}
