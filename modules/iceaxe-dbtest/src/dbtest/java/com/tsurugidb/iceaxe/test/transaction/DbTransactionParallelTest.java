package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * parallel transaction test
 */
// トランザクションtx1とtx2がある。
// tx1はtestテーブル、tx2はtest2テーブルのみにアクセスする。（お互いに無関係なテーブルの更新）
// tx1よりtx2の方が（処理件数が少ないので）早く終わる想定。
// このとき、tx1の途中でtx2を開始したらどうなるか？というテスト
class DbTransactionParallelTest extends DbTestTableTester {

    private static final String TEST2 = "test2";
    private static final int TEST_SIZE = 100;
    private static final int TEST2_SIZE = 2;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        dropTable(TEST2);
        createTest2Table();

        logInitEnd(info);
    }

    private static void createTest2Table() throws IOException, InterruptedException {
        var sql = CREATE_TEST_SQL.replace(TEST, TEST2);
        executeDdl(getSession(), sql);
    }

    @Test
    void testOccOcc() throws Exception {
        test(TgTxOption.ofOCC(), TgTxOption.ofOCC(), false);
    }

    @Test
    void testOccLtx() throws Exception {
        test(TgTxOption.ofOCC(), TgTxOption.ofLTX(TEST2), false);
    }

    @Test
    void testLtxOcc() throws Exception {
        test(TgTxOption.ofLTX(TEST), TgTxOption.ofOCC(), false);
    }

    @Test
    void testLtxLtx() throws Exception {
        test(TgTxOption.ofLTX(TEST), TgTxOption.ofLTX(TEST2), false);
    }

    @Test
    void testLtxLtxReadArea() throws Exception {
        // LTX同士でも、read areaを指定するとtx2はtx1を待たない
        var txOption1 = TgTxOption.ofLTX(TEST).addInclusiveReadArea(TEST);
        var txOption2 = TgTxOption.ofLTX(TEST2).addInclusiveReadArea(TEST2);
        test(txOption1, txOption2, false);
    }

    private void test(TgTxOption txOption1, TgTxOption txOption2, boolean wait) throws Exception {
        Tx2Thread[] thread = { null };

        try (var session = DbTestConnector.createSession(Long.MAX_VALUE, TimeUnit.NANOSECONDS); //
                var ps1 = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                var tx1 = session.createTransaction(txOption1)) {
            try {
//              LOG.info("tx1={}", tx1.getTransactionId());
                runInTransaction(tx1, ps1, thread, txOption2, wait);
                tx1.commit(TgCommitType.DEFAULT);
            } catch (Throwable t) {
                tx1.rollback();
                throw t;
            }
        }

        if (wait) {
            thread[0].join();
            assertTrue(thread[0].done);
        }

        assertEqualsTestTable(TEST_SIZE);
        assertEquals(TEST2_SIZE, selectCountFrom(TEST2));
    }

    private void runInTransaction(TsurugiTransaction tx1, TsurugiSqlPreparedStatement<TestEntity> ps1, Tx2Thread[] thread, TgTxOption txOption2, boolean wait)
            throws IOException, TsurugiTransactionException, InterruptedException {
        for (int i = 0; i < TEST_SIZE; i++) {
            if (i == 10) {
                // トランザクションの途中で、無関係なテーブルを扱う別トランザクションを開始する
                thread[0] = new Tx2Thread(txOption2);
                thread[0].start();
            }

            var entity = createTestEntity(i);
            tx1.executeAndGetCount(ps1, entity);

            if (i == TEST_SIZE - 1) {
                if (wait) {
                    // tx2は（tx1が終わるのを待つので）まだ実行中
                    assertFalse(thread[0].done);
                } else {
                    // Tx2Threadの方が短いので、先に終わっているはず
                    thread[0].join();
                    assertTrue(thread[0].done);
                }
            }
        }
    }

    private class Tx2Thread extends Thread {

        private final TgTxOption txOption2;
        public boolean done = false;

        public Tx2Thread(TgTxOption txOption2) {
            this.txOption2 = txOption2;
        }

        @Override
        public void run() {
            try {
                try (var session = DbTestConnector.createSession(Long.MAX_VALUE, TimeUnit.NANOSECONDS); //
                        var ps2 = session.createStatement(INSERT_SQL.replace(TEST, TEST2), INSERT_MAPPING); //
                        var tx2 = session.createTransaction(txOption2)) {
                    try {
//                      LOG.info("tx2={}", tx2.getTransactionId());
                        runInTransaction(tx2, ps2);
                        tx2.commit(TgCommitType.DEFAULT);
                    } catch (Throwable t) {
                        tx2.rollback();
                        throw t;
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            this.done = true;
        }

        private void runInTransaction(TsurugiTransaction tx2, TsurugiSqlPreparedStatement<TestEntity> ps2) throws IOException, TsurugiTransactionException, InterruptedException {
            for (int i = 0; i < TEST2_SIZE; i++) {
                var entity = createTestEntity(i);
                tx2.executeAndGetCount(ps2, entity);
            }
        }
    }
}
