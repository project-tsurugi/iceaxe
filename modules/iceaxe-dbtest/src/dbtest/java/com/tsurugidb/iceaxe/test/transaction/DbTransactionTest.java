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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.event.TsurugiTransactionEventListener;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;
import com.tsurugidb.tsubakuro.sql.TransactionStatus;

/**
 * transaction test
 */
class DbTransactionTest extends DbTestTableTester {

    private static final int SIZE = 2;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(info);
    }

    @Test
    void transactionId() throws Exception {
        var session = getSession();
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var id = transaction.getTransactionId();
            assertNotNull(id);
            assertFalse(id.isEmpty());
        }
    }

    @Test
    void transactionStatus_normal() throws Exception {
        var session = getSession();
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            var status = transaction.getTransactionStatus();
            assertTrue(status.isNormal());
            assertFalse(status.isError());
            assertNull(status.getDiagnosticCode());
            assertNull(status.getTransactionException());
            assertTrue(status.isTransactionFound());
            assertEquals(TransactionStatus.RUNNING, status.getLowTransactionStatus());
            assertEquals("", status.getTransactionStatusMessage());

            try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
                var entity = createTestEntity(SIZE);
                transaction.executeAndGetCount(ps, entity);

                var status2 = transaction.getTransactionStatus();
                assertTrue(status2.isNormal());
                assertFalse(status2.isError());
                assertNull(status2.getDiagnosticCode());
                assertNull(status2.getTransactionException());
                assertTrue(status.isTransactionFound());
                assertEquals(TransactionStatus.RUNNING, status2.getLowTransactionStatus());
                assertEquals("", status2.getTransactionStatusMessage());
            }
        }
    }

    @Test
    void transactionStatus_parseError() throws Exception {
        var session = getSession();
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            String sql = "insertinto " + TEST + " values(" + SIZE + ", 1, 'a')"; // parse error
            try (var ps = session.createStatement(sql)) {
                var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                    transaction.executeAndGetCount(ps);
                });
                assertEqualsCode(SqlServiceCode.SYNTAX_EXCEPTION, e);
            }
            var status = transaction.getTransactionStatus();
            assertFalse(status.isNormal());
            assertTrue(status.isError());
            assertEquals(SqlServiceCode.SYNTAX_EXCEPTION, status.getDiagnosticCode());
            assertNotNull(status.getTransactionException());
            assertTrue(status.isTransactionFound());
            assertEquals(TransactionStatus.ABORTED, status.getLowTransactionStatus());
            assertEquals("", status.getTransactionStatusMessage());

            try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
                var entity = createTestEntity(SIZE);
                var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                    transaction.executeAndGetCount(ps, entity);
                });
                assertEqualsCode(SqlServiceCode.INACTIVE_TRANSACTION_EXCEPTION, e);
                assertContains("Current transaction is inactive (maybe aborted already.)", e.getMessage());
            }
            var status2 = transaction.getTransactionStatus();
            assertFalse(status2.isNormal());
            assertTrue(status2.isError());
            assertEquals(SqlServiceCode.SYNTAX_EXCEPTION, status2.getDiagnosticCode());
            assertNotNull(status2.getTransactionException());
            assertTrue(status2.isTransactionFound());
            assertEquals(TransactionStatus.ABORTED, status2.getLowTransactionStatus());
            assertEquals("", status2.getTransactionStatusMessage());
        }
    }

    @Test
    void transactionStatus_parseError_prepared() throws Exception {
        var session = getSession();
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            String sql = "insertinto " + TEST + " values(" + SIZE + ", 1, 'a')"; // parse error
            try (var ps = session.createStatement(sql, TgParameterMapping.of())) {
                var e = assertThrowsExactly(TsurugiIOException.class, () -> {
                    transaction.executeAndGetCount(ps, TgBindParameters.of());
                });
                assertEqualsCode(SqlServiceCode.SYNTAX_EXCEPTION, e);
            }
            var status = transaction.getTransactionStatus();
            assertTrue(status.isNormal());
            assertFalse(status.isError());
            assertNull(status.getDiagnosticCode());
            assertNull(status.getTransactionException());
            assertTrue(status.isTransactionFound());
            assertEquals(TransactionStatus.RUNNING, status.getLowTransactionStatus());
            assertEquals("", status.getTransactionStatusMessage());

            try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
                var entity = createTestEntity(SIZE);
                transaction.executeAndGetCount(ps, entity);
            }
            var status2 = transaction.getTransactionStatus();
            assertTrue(status2.isNormal());
            assertFalse(status2.isError());
            assertNull(status2.getDiagnosticCode());
            assertNull(status2.getTransactionException());
            assertTrue(status2.isTransactionFound());
            assertEquals(TransactionStatus.RUNNING, status2.getLowTransactionStatus());
            assertEquals("", status2.getTransactionStatusMessage());
        }
    }

    @Test
    void transactionStatus_error() throws Exception {
        var session = getSession();
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
                var entity = createTestEntity(0);
                var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                    transaction.executeAndGetCount(ps, entity);
                });
                assertEqualsCode(SqlServiceCode.UNIQUE_CONSTRAINT_VIOLATION_EXCEPTION, e);
            }
            var status = transaction.getTransactionStatus();
            assertFalse(status.isNormal());
            assertTrue(status.isError());
            assertEquals(SqlServiceCode.UNIQUE_CONSTRAINT_VIOLATION_EXCEPTION, status.getDiagnosticCode());
            assertNotNull(status.getTransactionException());
            assertTrue(status.isTransactionFound());
            assertEquals(TransactionStatus.ABORTED, status.getLowTransactionStatus());
            assertEquals("", status.getTransactionStatusMessage());
        }
    }

    @Test
    void transactionStatus_error2() throws Exception {
        var session = getSession();
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
                var entity = createTestEntity(0);
                var e1 = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                    transaction.executeAndGetCount(ps, entity);
                });
                assertEqualsCode(SqlServiceCode.UNIQUE_CONSTRAINT_VIOLATION_EXCEPTION, e1);

                var e2 = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                    transaction.executeAndGetCount(ps, entity);
                });
                assertEqualsCode(SqlServiceCode.INACTIVE_TRANSACTION_EXCEPTION, e2);
                assertContains("Current transaction is inactive (maybe aborted already.)", e2.getMessage());
            }
            var status = transaction.getTransactionStatus();
            assertFalse(status.isNormal());
            assertTrue(status.isError());
            assertEquals(SqlServiceCode.UNIQUE_CONSTRAINT_VIOLATION_EXCEPTION, status.getDiagnosticCode());
            assertNotNull(status.getTransactionException());
            assertTrue(status.isTransactionFound());
            assertEquals(TransactionStatus.ABORTED, status.getLowTransactionStatus());
            assertEquals("", status.getTransactionStatusMessage());
        }
    }

    @Test
    void transactionStatus_afterCommit() throws Exception {
        var session = getSession();
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            transaction.commit(TgCommitType.DEFAULT);
            var status = transaction.getTransactionStatus();
            assertTrue(status.isNormal());
            assertNull(status.getTransactionException());
            assertTrue(status.isTransactionFound());
            assertEquals(TransactionStatus.STORED, status.getLowTransactionStatus());
            assertEquals("", status.getTransactionStatusMessage());
        }
    }

    @Test
    void transactionStatus_afterRollback() throws Exception {
        var session = getSession();
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            transaction.rollback();
            var status = transaction.getTransactionStatus();
            assertTrue(status.isNormal());
            assertNull(status.getTransactionException());
            assertTrue(status.isTransactionFound());
            assertEquals(TransactionStatus.ABORTED, status.getLowTransactionStatus());
            assertEquals("", status.getTransactionStatusMessage());
        }
    }

    @Test
    void transactionStatus_afterClose() throws Exception {
        var session = getSession();
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            transaction.close();
            var e = assertThrowsExactly(IceaxeIOException.class, () -> {
                transaction.getTransactionStatus();
            });
            assertEqualsCode(IceaxeErrorCode.TX_ALREADY_CLOSED, e);
        }
    }

    @Test
    void commit() throws Exception {
        var session = getSession();
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            assertSelect(SIZE, session, transaction);

            var entity = createTestEntity(SIZE);
            transaction.executeAndGetCount(ps, entity);

            assertSelect(SIZE + 1, session, transaction);

            transaction.commit(TgCommitType.DEFAULT);
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void commitTm() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            tm.execute(transaction -> {
                assertSelect(SIZE, session, transaction);

                var entity = createTestEntity(SIZE);
                transaction.executeAndGetCount(ps, entity);

                assertSelect(SIZE + 1, session, transaction);
            });
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void rollback() throws Exception {
        var session = getSession();
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            assertSelect(SIZE, session, transaction);

            var entity = createTestEntity(SIZE);
            transaction.executeAndGetCount(ps, entity);

            assertSelect(SIZE + 1, session, transaction);

            transaction.rollback();
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void rollbackTmByException() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            assertThrowsExactly(IOException.class, () -> {
                tm.execute((TsurugiTransactionAction) transaction -> {
                    assertSelect(SIZE, session, transaction);

                    var entity = createTestEntity(SIZE);
                    transaction.executeAndGetCount(ps, entity);

                    assertSelect(SIZE + 1, session, transaction);

                    throw new IOException("test");
                });
            });
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void rollbackTmExplicit() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            tm.execute(transaction -> {
                assertSelect(SIZE, session, transaction);

                var entity = createTestEntity(SIZE);
                transaction.executeAndGetCount(ps, entity);

                assertSelect(SIZE + 1, session, transaction);

                transaction.rollback();
            });
        }

        assertEqualsTestTable(SIZE);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void rollbackOnClose(boolean rollback) throws Exception {
        var session = getSession();

        var txOption = TgTxOption.ofOCC();
        txOption.setRollbackOnTransactionClose(rollback);

        boolean[] rollbackCalled = { false };
        try (var transaction = session.createTransaction(txOption)) {
            transaction.addEventListener(new TsurugiTransactionEventListener() {
                @Override
                public void rollbackStart(TsurugiTransaction transaction) {
                    rollbackCalled[0] = true;
                }
            });

            transaction.getLowTransaction();
        }

        if (rollback) {
            assertTrue(rollbackCalled[0]);
        } else {
            assertFalse(rollbackCalled[0]);
        }
    }

    private static void assertSelect(int expected, TsurugiSession session, TsurugiTransaction transaction) throws IOException, TsurugiTransactionException, InterruptedException {
        try (var ps = session.createQuery(SELECT_SQL)) {
            var list = transaction.executeAndGetList(ps);
            assertEquals(expected, list.size());
        }
    }
}
