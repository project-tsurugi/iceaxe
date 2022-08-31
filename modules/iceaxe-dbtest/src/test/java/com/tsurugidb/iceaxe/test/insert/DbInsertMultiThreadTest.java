package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;

/**
 * multi thread insert test
 */
class DbInsertMultiThreadTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void insertSingleTxOcc() throws IOException, InterruptedException {
        insertSingleTx(1000, 30, TgTxOption.ofOCC());
    }

    @Test
    void insertSingleTxLtx() throws IOException, InterruptedException {
        insertSingleTx(1000, 30, TgTxOption.ofLTX(TEST));
    }

    @Test
    void insertMultiTxOcc() throws IOException, InterruptedException {
        insertMultiTx(1000, 30, TgTxOption.ofOCC());
    }

    @Test
    @Disabled // TODO remove Disabled
    void insertMultiTxLtx2() throws IOException, InterruptedException {
        insertMultiTx(100, 2, TgTxOption.ofLTX(TEST));
    }

    @Test
    @Disabled // TODO remove Disabled
    void insertMultiTxLtx3() throws IOException, InterruptedException {
        insertMultiTx(100, 3, TgTxOption.ofLTX(TEST));
    }

    /**
     * single transaction, parallel insert
     */
    private void insertSingleTx(int recordSize, int threadSize, TgTxOption tx) throws IOException {
        var session = getSession();
        var tm = session.createTransactionManager(tx);
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            tm.execute(transaction -> {
                var list = new ArrayList<InsertSingleTxThread>(threadSize);
                for (int i = 0; i < threadSize; i++) {
                    var thread = new InsertSingleTxThread(ps, i, threadSize, recordSize, transaction);
                    list.add(thread);
                    thread.start();
                }
                for (var thread : list) {
                    try {
                        thread.join();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        var actual = selectCountFromTest();
        assertEquals(recordSize, actual);
    }

    private static class InsertSingleTxThread extends Thread {

        private final TsurugiPreparedStatementUpdate1<TestEntity> ps;
        private final int number;
        private final int threadSize;
        private final int recordSize;
        private final TsurugiTransaction transaction;

        public InsertSingleTxThread(TsurugiPreparedStatementUpdate1<TestEntity> ps, int number, int threadSize, int recordSize, TsurugiTransaction transaction) {
            this.ps = ps;
            this.number = number;
            this.threadSize = threadSize;
            this.recordSize = recordSize;
            this.transaction = transaction;
        }

        @Override
        public void run() {
            try {
                runInTransaction();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (TsurugiTransactionException e) {
                throw new RuntimeException(e);
            }
        }

        private void runInTransaction() throws IOException, TsurugiTransactionException {
            for (int i = 0; i < recordSize; i++) {
                if (i % threadSize == number) {
                    var entity = createTestEntity(i);
                    ps.executeAndGetCount(transaction, entity);
                }
            }
        }
    }

    /**
     * transaction per thread, parallel insert
     */
    private void insertMultiTx(int recordSize, int threadSize, TgTxOption tx) throws IOException {
        var session = getSession();
        var tm = session.createTransactionManager(tx);
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            var list = new ArrayList<InsertMultiTxThread>(threadSize);
            for (int i = 0; i < threadSize; i++) {
                var thread = new InsertMultiTxThread(ps, i, threadSize, recordSize, tm);
                list.add(thread);
                thread.start();
            }
            for (var thread : list) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        var actual = selectCountFromTest();
        assertEquals(recordSize, actual);
    }

    private static class InsertMultiTxThread extends Thread {

        private final TsurugiPreparedStatementUpdate1<TestEntity> ps;
        private final int number;
        private final int threadSize;
        private final int recordSize;
        private final TsurugiTransactionManager tm;

        public InsertMultiTxThread(TsurugiPreparedStatementUpdate1<TestEntity> ps, int number, int threadSize, int recordSize, TsurugiTransactionManager tm) {
            this.ps = ps;
            this.number = number;
            this.threadSize = threadSize;
            this.recordSize = recordSize;
            this.tm = tm;
        }

        @Override
        public void run() {
            try {
                tm.execute(transaction -> {
                    runInTransaction(transaction);
                });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private void runInTransaction(TsurugiTransaction transaction) throws IOException, TsurugiTransactionException {
            for (int i = 0; i < recordSize; i++) {
                if (i % threadSize == number) {
                    var entity = createTestEntity(i);
                    ps.executeAndGetCount(transaction, entity);
                }
            }
        }
    }
}
