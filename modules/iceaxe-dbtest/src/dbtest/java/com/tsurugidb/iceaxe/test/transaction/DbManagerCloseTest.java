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
package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmRetryOverIOException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.util.InterruptedRuntimeException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;
import com.tsurugidb.tsubakuro.sql.exception.CcException;

/**
 * TransactionManager transaction close test
 */
class DbManagerCloseTest extends DbTestTableTester {

    private static final int SIZE = 2;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbManagerCloseTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(LOG, info);
    }

    @Test
    void execute_commit1() throws Exception {
        execute1(false);
    }

    @Test
    void execute_rollback1() throws Exception {
        execute1(true);
    }

    private void execute1(boolean rollback) throws IOException, InterruptedException {
        var session = getSession();
        var setting = TgTmSetting.of(TgTxOption.ofOCC(), 3);
        var tm = session.createTransactionManager(setting);

        var txList = new ArrayList<TsurugiTransaction>();
        tm.execute(transaction -> {
            txList.add(transaction);
            if (rollback) {
                transaction.rollback();
            }
        });

        assertEquals(1, txList.size());
        {
            var transaction = txList.get(0);
            if (rollback) {
                assertFalse(transaction.isCommitted());
                assertTrue(transaction.isRollbacked());
            } else {
                assertTrue(transaction.isCommitted());
                assertFalse(transaction.isRollbacked());
            }
            assertTrue(transaction.isClosed());
        }
    }

    private final TsurugiTransactionException RETRY_EXCEPTION = new TsurugiTransactionException(new CcException(SqlServiceCode.CC_EXCEPTION));

    @Test
    void execute_commit2() throws Exception {
        execute2(false, RETRY_EXCEPTION);
        execute2(false, new TsurugiTransactionRuntimeException(RETRY_EXCEPTION));
    }

    @Test
    void execute_rollback2() throws Exception {
        execute2(true, RETRY_EXCEPTION);
        execute2(true, new TsurugiTransactionRuntimeException(RETRY_EXCEPTION));
    }

    private void execute2(boolean rollback, Throwable t) throws IOException, InterruptedException {
        var session = getSession();
        var setting = TgTmSetting.of(TgTxOption.ofOCC(), 3);
        var tm = session.createTransactionManager(setting);

        var txList = new ArrayList<TsurugiTransaction>();
        tm.execute(transaction -> {
            txList.add(transaction);
            if (transaction.getAttempt() == 0) {
                if (t instanceof TsurugiTransactionException) {
                    throw (TsurugiTransactionException) t;
                }
                if (t instanceof RuntimeException) {
                    throw (RuntimeException) t;
                }
                throw new AssertionError(t);
            }
            if (rollback) {
                transaction.rollback();
            }
        });

        assertEquals(2, txList.size());
        {
            var transaction = txList.get(0);
            assertFalse(transaction.isCommitted());
            assertTrue(transaction.isRollbacked());
            assertTrue(transaction.isClosed());
        }
        {
            var transaction = txList.get(1);
            if (rollback) {
                assertFalse(transaction.isCommitted());
                assertTrue(transaction.isRollbacked());
            } else {
                assertTrue(transaction.isCommitted());
                assertFalse(transaction.isRollbacked());
            }
            assertTrue(transaction.isClosed());
        }
    }

    @Test
    void execute_retryOver() throws Exception {
        execute_retryOver(true);
    }

    private void execute_retryOver(boolean execute) throws IOException {
        var session = getSession();
        var setting = TgTmSetting.of(TgTxOption.ofOCC(), 3);
        var tm = session.createTransactionManager(setting);

        var txList = new ArrayList<TsurugiTransaction>();
        var e = assertThrows(TsurugiTmRetryOverIOException.class, () -> {
            TsurugiTransactionAction action = transaction -> {
                txList.add(transaction);
                throw RETRY_EXCEPTION;
            };
            if (execute) {
                tm.execute(action);
            } else {
                tm.executeAndGetTransaction(action);
            }
        });
        assertEqualsCode(SqlServiceCode.CC_EXCEPTION, e);

        assertEquals(3, txList.size());
        for (var transaction : txList) {
            assertFalse(transaction.isCommitted());
            assertTrue(transaction.isRollbacked());
            assertTrue(transaction.isClosed());
        }
    }

    @Test
    void execute_RuntimeException() throws Exception {
        execute_RuntimeException(true);
    }

    private void execute_RuntimeException(boolean execute) throws IOException {
        var session = getSession();
        var setting = TgTmSetting.of(TgTxOption.ofOCC(), 3);
        var tm = session.createTransactionManager(setting);

        var txList = new ArrayList<TsurugiTransaction>();
        var e = assertThrowsExactly(RuntimeException.class, () -> {
            TsurugiTransactionAction action = transaction -> {
                txList.add(transaction);
                if (transaction.getAttempt() == 0) {
                    throw RETRY_EXCEPTION;
                }
                throw new RuntimeException("test exception");
            };
            if (execute) {
                tm.execute(action);
            } else {
                tm.executeAndGetTransaction(action);
            }
        });
        assertEquals("test exception", e.getMessage());

        assertEquals(2, txList.size());
        for (var transaction : txList) {
            assertFalse(transaction.isCommitted());
            assertTrue(transaction.isRollbacked());
            assertTrue(transaction.isClosed());
        }
    }

    @Test
    void execute_InterruptedRuntimeException() throws Exception {
        execute_InterruptedRuntimeException(true);
    }

    private void execute_InterruptedRuntimeException(boolean execute) throws Exception {
        var session = getSession();
        var setting = TgTmSetting.of(TgTxOption.ofOCC(), 3);
        var tm = session.createTransactionManager(setting);

        var txList = new ArrayList<TsurugiTransaction>();
        var e = assertThrows(InterruptedException.class, () -> {
            TsurugiTransactionAction action = transaction -> {
                txList.add(transaction);
                if (transaction.getAttempt() == 0) {
                    throw RETRY_EXCEPTION;
                }
                throw new InterruptedRuntimeException(new InterruptedException("test exception"));
            };
            if (execute) {
                tm.execute(action);
            } else {
                tm.executeAndGetTransaction(action);
            }
        });
        assertEquals("test exception", e.getMessage());

        assertEquals(2, txList.size());
        for (var transaction : txList) {
            assertFalse(transaction.isCommitted());
            assertTrue(transaction.isRollbacked());
            assertTrue(transaction.isClosed());
        }
    }

    @Test
    void execute_Error() throws Exception {
        execute_Error(true);
    }

    private void execute_Error(boolean execute) throws IOException {
        var session = getSession();
        var setting = TgTmSetting.of(TgTxOption.ofOCC(), 3);
        var tm = session.createTransactionManager(setting);

        var txList = new ArrayList<TsurugiTransaction>();
        var e = assertThrowsExactly(Error.class, () -> {
            TsurugiTransactionAction action = transaction -> {
                txList.add(transaction);
                if (transaction.getAttempt() == 0) {
                    throw RETRY_EXCEPTION;
                }
                throw new Error("test exception");
            };
            if (execute) {
                tm.execute(action);
            } else {
                tm.executeAndGetTransaction(action);
            }
        });
        assertEquals("test exception", e.getMessage());

        assertEquals(2, txList.size());
        for (var transaction : txList) {
            assertFalse(transaction.isCommitted());
            assertTrue(transaction.isRollbacked());
            assertTrue(transaction.isClosed());
        }
    }

    @Test
    void executeAndGetTransaction_commit1() throws Exception {
        executeAndGetTransaction1(false);
    }

    @Test
    void executeAndGetTransaction_rollback1() throws Exception {
        executeAndGetTransaction1(true);
    }

    private void executeAndGetTransaction1(boolean rollback) throws IOException, InterruptedException {
        var session = getSession();
        var setting = TgTmSetting.of(TgTxOption.ofOCC(), 3);
        var tm = session.createTransactionManager(setting);

        var txList = new ArrayList<TsurugiTransaction>();
        var returnTransaction = tm.executeAndGetTransaction(transaction -> {
            txList.add(transaction);
            if (rollback) {
                transaction.rollback();
            }
        });

        assertEquals(1, txList.size());
        {
            var transaction = txList.get(0);
            assertSame(returnTransaction, transaction);
            if (rollback) {
                assertFalse(transaction.isCommitted());
                assertTrue(transaction.isRollbacked());
            } else {
                assertTrue(transaction.isCommitted());
                assertFalse(transaction.isRollbacked());
            }
            assertFalse(transaction.isClosed());
            transaction.close();
        }
    }

    @Test
    void executeAndGetTransaction_commit2() throws Exception {
        executeAndGetTransaction2(false, RETRY_EXCEPTION);
        executeAndGetTransaction2(false, new TsurugiTransactionRuntimeException(RETRY_EXCEPTION));
    }

    @Test
    void executeAndGetTransaction_rollback2() throws Exception {
        executeAndGetTransaction2(true, RETRY_EXCEPTION);
        executeAndGetTransaction2(true, new TsurugiTransactionRuntimeException(RETRY_EXCEPTION));
    }

    private void executeAndGetTransaction2(boolean rollback, Throwable t) throws IOException, InterruptedException {
        var session = getSession();
        var setting = TgTmSetting.of(TgTxOption.ofOCC(), 3);
        var tm = session.createTransactionManager(setting);

        var txList = new ArrayList<TsurugiTransaction>();
        var returnTransaction = tm.executeAndGetTransaction(transaction -> {
            txList.add(transaction);
            if (transaction.getAttempt() == 0) {
                if (t instanceof TsurugiTransactionException) {
                    throw (TsurugiTransactionException) t;
                }
                if (t instanceof RuntimeException) {
                    throw (RuntimeException) t;
                }
                throw new AssertionError(t);
            }
            if (rollback) {
                transaction.rollback();
            }
        });

        assertEquals(2, txList.size());
        {
            var transaction = txList.get(0);
            assertFalse(transaction.isCommitted());
            assertTrue(transaction.isRollbacked());
            assertTrue(transaction.isClosed());
        }
        {
            var transaction = txList.get(1);
            assertSame(returnTransaction, transaction);
            if (rollback) {
                assertFalse(transaction.isCommitted());
                assertTrue(transaction.isRollbacked());
            } else {
                assertTrue(transaction.isCommitted());
                assertFalse(transaction.isRollbacked());
            }
            assertFalse(transaction.isClosed());
            transaction.close();
        }
    }

    @Test
    void executeAndGetTransaction_retryOver() throws Exception {
        execute_retryOver(false);
    }

    @Test
    void executeAndGetTransaction_RuntimeException() throws Exception {
        execute_RuntimeException(false);
    }

    @Test
    void executeAndGetTransaction_InterruptedRuntimeException() throws Exception {
        execute_InterruptedRuntimeException(false);
    }

    @Test
    void executeAndGetTransaction_Error() throws Exception {
        execute_Error(false);
    }
}
