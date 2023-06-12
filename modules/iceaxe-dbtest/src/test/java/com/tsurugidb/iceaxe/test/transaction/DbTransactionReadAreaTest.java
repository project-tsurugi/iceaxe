package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * transaction read area test
 */
class DbTransactionReadAreaTest extends DbTestTableTester {

    private static final String TEST2 = "test2";
    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        dropTable(TEST2);
        createTest2Table();
        insertTest2Table(SIZE);

        logInitEnd(info);
    }

    private static void createTest2Table() throws IOException, InterruptedException {
        var sql = CREATE_TEST_SQL.replace(TEST, TEST2);
        executeDdl(getSession(), sql);
    }

    protected static void insertTest2Table(int size) throws IOException, InterruptedException {
        var sql = INSERT_SQL.replace(TEST, TEST2);

        var session = getSession();
        var tm = createTransactionManagerOcc(session, 3);
        try (var ps = session.createStatement(sql, INSERT_MAPPING)) {
            tm.execute(transaction -> {
                for (int i = 0; i < size; i++) {
                    var entity = createTestEntity(i);
                    transaction.executeAndGetCount(ps, entity);
                }
                return;
            });
        }
    }

    private static final TgTxOption LTX1 = TgTxOption.ofLTX(TEST).addInclusiveReadArea(TEST);
    private static final TgTxOption LTX2 = TgTxOption.ofLTX(TEST2).addInclusiveReadArea(TEST2);
    private static final TgTxOption LTX1_NOTHING = TgTxOption.ofLTX(TEST);
    private static final TgTxOption LTX2_NOTHING = TgTxOption.ofLTX(TEST2);

    private static final int KEY = 1;
    private static final String SELECT1_SQL = SELECT_SQL + " where foo = " + KEY;
    private static final String SELECT2_SQL = SELECT1_SQL.replace(TEST, TEST2);
    private static final String UPDATE1_SQL = "update " + TEST + " set bar=789 where foo = " + KEY;
    private static final String UPDATE2_SQL = UPDATE1_SQL.replace(TEST, TEST2);

    @Test
    void readArea1Select_nothing() throws Exception {
        var session = getSession();
        try (var select2Ps = session.createQuery(SELECT2_SQL, SELECT_MAPPING)) {
            try (var tx1 = session.createTransaction(LTX1_NOTHING)) {
                for (int i = 0; i < 3; i++) {
                    tx1.executeAndGetList(select2Ps);
                }
                tx1.commit(TgCommitType.DEFAULT);
            }
        }
    }

    @Test
    @Disabled // TODO remove Disabled. read area修正待ち
    void readArea1Select() throws Exception {
        var session = getSession();
        try (var select2Ps = session.createQuery(SELECT2_SQL, SELECT_MAPPING)) {
            try (var tx1 = session.createTransaction(LTX1)) {
                for (int i = 0; i < 3; i++) {
                    var e = assertThrowsExactly(TsurugiTransactionException.class, () -> tx1.executeAndGetList(select2Ps));
                    assertEqualsCode(SqlServiceCode.ERR_ILLEGAL_OPERATION, e);
                }
                tx1.commit(TgCommitType.DEFAULT);
            }
        }
    }

    @Test
    void readArea1Update_nothing() throws Exception {
        var session = getSession();
        try (var update1Ps = session.createStatement(UPDATE1_SQL)) {
            try (var tx1 = session.createTransaction(LTX1_NOTHING)) {
                for (int i = 1; i < 3; i++) {
                    tx1.executeAndGetCount(update1Ps);
                }
                tx1.commit(TgCommitType.DEFAULT);
            }
        }
    }

    @Test
    @Disabled // TODO remove Disabled. read area修正待ち
    void readArea1Update() throws Exception {
        var session = getSession();
        try (var update1Ps = session.createStatement(UPDATE1_SQL)) {
            try (var tx1 = session.createTransaction(LTX1)) {
                tx1.executeAndGetCount(update1Ps);

                for (int i = 0; i < 3; i++) {
                    var e = assertThrowsExactly(TsurugiTransactionException.class, () -> tx1.executeAndGetCount(update1Ps));
                    assertEqualsCode(SqlServiceCode.ERR_ILLEGAL_OPERATION, e);
                }
            }
        }
    }

    @Test
    void readArea2_nothing() throws Exception {
        var session = getSession();
        try (var update1Ps = session.createStatement(UPDATE1_SQL); //
                var update2Ps = session.createStatement(UPDATE2_SQL)) {
            try (var tx1 = session.createTransaction(LTX1_NOTHING)) {
                tx1.executeAndGetCount(update1Ps);

                try (var tx2 = session.createTransaction(LTX2_NOTHING)) {
                    tx2.executeAndGetCount(update2Ps);
//x                 tx2.commit(TgCommitType.DEFAULT);

                    var start2 = new AtomicBoolean(false);
                    var done2 = new AtomicBoolean(false);
                    var future2 = Executors.newFixedThreadPool(1).submit(() -> {
                        start2.set(true);
                        try {
                            tx2.commit(TgCommitType.DEFAULT);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e.getMessage(), e);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } catch (TsurugiTransactionException e) {
                            throw new TsurugiTransactionRuntimeException(e);
                        } finally {
                            done2.set(true);
                        }
                    });

                    while (!start2.get())
                        ;
                    assertFalse(done2.get());
                    tx1.commit(TgCommitType.DEFAULT);

                    future2.get();
                }
            }
        }
    }

    @Test
    @Disabled // TODO remove Disabled. read area修正待ち
    void readArea2() throws Exception {
        var session = getSession();
        try (var update1Ps = session.createStatement(UPDATE1_SQL); //
                var update2Ps = session.createStatement(UPDATE2_SQL)) {
            try (var tx1 = session.createTransaction(LTX1)) {
                tx1.executeAndGetCount(update1Ps);

                try (var tx2 = session.createTransaction(LTX2)) {
                    tx2.executeAndGetCount(update2Ps);
                    tx2.commit(TgCommitType.DEFAULT);

                    tx1.commit(TgCommitType.DEFAULT);
                }
            }
        }
    }
}
