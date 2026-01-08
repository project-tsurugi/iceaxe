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
package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.exception.TsurugiExceptionUtil;
import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * select count test
 */
class DbSelectCountTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();

        logInitEnd(info);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 3 })
    void count_converter(int size) throws Exception {
        count(size, TgResultMapping.of(record -> record.nextInt()));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 3 })
    void count_singleColumn_class(int size) throws Exception {
        count(size, TgResultMapping.ofSingle(int.class));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 3 })
    void count_singleColumn_type(int size) throws Exception {
        count(size, TgResultMapping.ofSingle(TgDataType.INT));
    }

    private void count(int size, TgResultMapping<Integer> resultMapping) throws IOException, InterruptedException {
        insertTestTable(size);

        var sql = "select count(*) from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, resultMapping)) {
            int count = tm.executeAndFindRecord(ps).get();
            assertEquals(size, count);
        }
    }

    @RepeatedTest(30)
    void countWhileInsertOcc() throws Exception {
        countWhileInsert(TgTxOption.ofOCC());
    }

    @RepeatedTest(30)
    void countWhileInsertLtx() throws Exception {
        countWhileInsert(TgTxOption.ofLTX());
    }

    @RepeatedTest(30)
    void countWhileInsertRtx() throws Exception {
        countWhileInsert(TgTxOption.ofRTX());
    }

    private static final int COMMIT_SIZE = 100;

    private void countWhileInsert(TgTxOption txOption) throws Exception {
        var sql = "select count(*) from " + TEST;

        var session = getSession();
        try (var ps = session.createQuery(sql, TgParameterMapping.of(), TgResultMapping.ofSingle(int.class))) {
            var insertThread = new InsertThread();
            insertThread.start();

            Throwable occurred = null;
            try {
                while (!insertThread.end) {
                    try (var transaction = session.createTransaction(txOption)) {
                        int count = transaction.executeAndFindRecord(ps, null).orElse(0);

                        String message = null;
                        if (count % COMMIT_SIZE != 0) {
                            message = MessageFormat.format("count={0} (COMMIT_SIZE={1})", count, COMMIT_SIZE);
//                          fail(message);
                            LOG.info(message);
                        }

                        if (message == null) {
                            transaction.commit(TgCommitType.DEFAULT);
                        } else {
                            try {
                                var e = assertThrows(TsurugiTransactionException.class, () -> {
                                    transaction.commit(TgCommitType.DEFAULT);
                                });
                                assertEqualsCode(SqlServiceCode.CC_EXCEPTION, e);
                                continue;
                            } catch (Throwable t) {
                                t.addSuppressed(new AssertionError(message));
                                throw t;
                            }
                        }
                    } catch (TsurugiTransactionException e) {
                        var exceptionUtil = TsurugiExceptionUtil.getInstance();
                        if (exceptionUtil.isSerializationFailure(e)) {
                            continue;
                        }
                        throw e;
                    }
                }
            } catch (Throwable e) {
                occurred = e;
                throw e;
            } finally {
                insertThread.join();

                var e = insertThread.exception;
                if (e != null) {
                    if (occurred != null) {
                        occurred.addSuppressed(e);
                    } else {
                        if (e instanceof Exception) {
                            throw (Exception) e;
                        }
                        throw new Exception(e);
                    }
                }
            }
        }
    }

    private static class InsertThread extends Thread {
        private volatile boolean end = false;
        private Throwable exception;

        @Override
        public void run() {
            try {
                execute();
            } catch (Throwable e) {
                this.exception = e;
            } finally {
                this.end = true;
            }
        }

        private void execute() throws Exception {
            try (var session = DbTestConnector.createSession()) {
                var tm = session.createTransactionManager(TgTxOption.ofOCC());
                try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
                    var foo = new AtomicInteger(0);
                    for (int i = 0; i < 20; i++) {
                        tm.execute((TsurugiTransactionAction) transaction -> {
                            for (int j = 0; j < COMMIT_SIZE; j++) {
                                var entity = createTestEntity(foo.getAndIncrement());
                                transaction.executeAndGetCount(ps, entity);
                            }
                        });
                    }
                }
            }
        }
    }
}
