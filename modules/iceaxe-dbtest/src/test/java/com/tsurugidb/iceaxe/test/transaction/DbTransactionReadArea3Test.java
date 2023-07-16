package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * transaction read area test
 */
@Disabled // TODO remove Disabled. ERR_DATA_CORRUPTIONが発生する
class DbTransactionReadArea3Test extends DbTestTableTester {

    private static final String TABLE_A = TEST;
    private static final String TABLE_B = "test2";
    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        dropTable(TABLE_B);
        createTest2Table();
        insertTest2Table(SIZE);

        logInitEnd(info);
    }

    private static void createTest2Table() throws IOException, InterruptedException {
        var sql = CREATE_TEST_SQL.replace(TEST, TABLE_B);
        executeDdl(getSession(), sql);
    }

    protected static void insertTest2Table(int size) throws IOException, InterruptedException {
        var sql = INSERT_SQL.replace(TEST, TABLE_B);

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

    private static final TestEntity ENTITY0 = new TestEntity(SIZE + 1, 0, "0");
    private static final String INSERT_A_SQL = INSERT_SQL;
    private static final String SELECT_A_SQL = SELECT_SQL + " where foo=" + ENTITY0.getFoo();
    private static final String INSERT_B_SQL = INSERT_SQL.replace(TEST, TABLE_B);
    private static final String SELECT_B_SQL = SELECT_A_SQL.replace(TABLE_A, TABLE_B);

    @Test
    void case1() throws Exception {
        var session = getSession();
        try (var insertAps = session.createStatement(INSERT_A_SQL, INSERT_MAPPING); //
                var selectAps = session.createQuery(SELECT_A_SQL, SELECT_MAPPING); //
                var insertBps = session.createStatement(INSERT_B_SQL, INSERT_MAPPING)) {
            try (var tx1 = session.createTransaction(TgTxOption.ofLTX(TABLE_A).label("t1"))) {
                tx1.getTransactionId();
                try (var tx2 = session.createTransaction(TgTxOption.ofLTX().addExclusiveReadArea(TABLE_B).label("t2"))) {
                    tx2.getTransactionId();
                    try (var tx3 = session.createTransaction(TgTxOption.ofLTX(TABLE_B).label("t3"))) {
                        tx3.getTransactionId();

                        tx1.executeAndGetCount(insertAps, ENTITY0);

                        tx3.executeAndGetList(selectAps);
                        tx3.executeAndGetCount(insertBps, ENTITY0);
                        tx3.commit(TgCommitType.DEFAULT);

                        tx1.commit(TgCommitType.DEFAULT);
                    }
                }
            }
        }
    }

    @Test
    void case2() throws Exception {
        var session = getSession();
        try (var insertAps = session.createStatement(INSERT_A_SQL, INSERT_MAPPING); //
                var selectAps = session.createQuery(SELECT_A_SQL, SELECT_MAPPING); //
                var insertBps = session.createStatement(INSERT_B_SQL, INSERT_MAPPING); //
                var selectBps = session.createQuery(SELECT_B_SQL, SELECT_MAPPING)) {
            try (var tx1 = session.createTransaction(TgTxOption.ofLTX(TABLE_A).label("t1"))) {
                tx1.getTransactionId();
                try (var tx2 = session.createTransaction(TgTxOption.ofLTX().label("t2"))) {
                    tx2.getTransactionId();
                    try (var tx3 = session.createTransaction(TgTxOption.ofLTX(TABLE_B).label("t3"))) {
                        tx3.getTransactionId();

                        tx1.executeAndGetCount(insertAps, ENTITY0);

                        tx3.executeAndGetList(selectAps);
                        tx3.executeAndGetCount(insertBps, ENTITY0);
                        var future3 = executeFuture(() -> {
                            tx3.commit(TgCommitType.DEFAULT); // wait for tx2
                            return null;
                        });

                        tx1.commit(TgCommitType.DEFAULT);

                        tx2.executeAndGetList(selectBps);

                        // before tx2 commit
                        var e3 = assertThrows(TsurugiTransactionException.class, () -> future3.get());
                        assertEqualsCode(null, e3); // TODO sqlCode

                        tx2.commit(TgCommitType.DEFAULT);
                    }
                }
            }
        }
    }
}
