/*
 * Copyright 2023-2026 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * multi thread insert test
 */
class DbInsertMultiThreadTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();

        logInitEnd(info);
    }

    @Test
    void insertSingleTxOcc() throws Exception {
        insertSingleTx(1000, 30, TgTxOption.ofOCC());
    }

    @Test
    void insertSingleTxLtx() throws Exception {
        insertSingleTx(1000, 30, TgTxOption.ofLTX(TEST));
    }

    @Test
    void insertMultiTxOcc() throws Exception {
        insertMultiTx(1000, 30, TgTxOption.ofOCC());
    }

    @ParameterizedTest
    @ValueSource(ints = { 2, 3, 8, 30 })
    void insertMultiTxLtx(int threadSize) throws Exception {
        insertMultiTx(1000, threadSize, TgTxOption.ofLTX(TEST));
    }

    /**
     * single transaction, parallel insert
     */
    private void insertSingleTx(int recordSize, int threadSize, TgTxOption tx) throws IOException, InterruptedException {
        var session = getSession();
        var tm = session.createTransactionManager(tx);
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
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

        private final TsurugiSqlPreparedStatement<TestEntity> ps;
        private final int number;
        private final int threadSize;
        private final int recordSize;
        private final TsurugiTransaction transaction;

        public InsertSingleTxThread(TsurugiSqlPreparedStatement<TestEntity> ps, int number, int threadSize, int recordSize, TsurugiTransaction transaction) {
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
            } catch (InterruptedException | TsurugiTransactionException e) {
                throw new RuntimeException(e);
            }
        }

        private void runInTransaction() throws IOException, InterruptedException, TsurugiTransactionException {
            for (int i = 0; i < recordSize; i++) {
                if (i % threadSize == number) {
                    var entity = createTestEntity(i);
                    transaction.executeAndGetCount(ps, entity);
                }
            }
        }
    }

    /**
     * transaction per thread, parallel insert
     */
    private void insertMultiTx(int recordSize, int threadSize, TgTxOption tx) throws IOException, InterruptedException {
        var session = getSession();
        var tm = session.createTransactionManager(tx);
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
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

        private final TsurugiSqlPreparedStatement<TestEntity> ps;
        private final int number;
        private final int threadSize;
        private final int recordSize;
        private final TsurugiTransactionManager tm;

        public InsertMultiTxThread(TsurugiSqlPreparedStatement<TestEntity> ps, int number, int threadSize, int recordSize, TsurugiTransactionManager tm) {
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
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        private void runInTransaction(TsurugiTransaction transaction) throws IOException, InterruptedException, TsurugiTransactionException {
            for (int i = 0; i < recordSize; i++) {
                if (i % threadSize == number) {
                    var entity = createTestEntity(i);
                    transaction.executeAndGetCount(ps, entity);
                }
            }
        }
    }
}
