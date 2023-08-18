package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ResponseBox;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * transaction test
 */
class DbTransactionTest extends DbTestTableTester {

    private static final int SIZE = 2;
    private static final int ATTEMPT_SIZE = ResponseBox.responseBoxSize() + 100;

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
            assertNull(status.getLowSqlServiceException());

            try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
                var entity = createTestEntity(SIZE);
                transaction.executeAndGetCount(ps, entity);

                var status2 = transaction.getTransactionStatus();
                assertTrue(status2.isNormal());
                assertFalse(status2.isError());
                assertNull(status2.getDiagnosticCode());
                assertNull(status2.getLowSqlServiceException());
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
                assertEqualsCode(SqlServiceCode.ERR_PARSE_ERROR, e);
            }
            var status = transaction.getTransactionStatus();
            assertTrue(status.isNormal());
            assertFalse(status.isError());
            assertNull(status.getDiagnosticCode());
            assertNull(status.getLowSqlServiceException());
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
                assertEqualsCode(SqlServiceCode.ERR_UNIQUE_CONSTRAINT_VIOLATION, e);
            }
            var status = transaction.getTransactionStatus();
            assertFalse(status.isNormal());
            assertTrue(status.isError());
            assertEquals(SqlServiceCode.ERR_UNIQUE_CONSTRAINT_VIOLATION, status.getDiagnosticCode());
            assertNotNull(status.getLowSqlServiceException());
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
                assertEqualsCode(SqlServiceCode.ERR_UNIQUE_CONSTRAINT_VIOLATION, e1);

                var e2 = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                    transaction.executeAndGetCount(ps, entity);
                });
                assertEqualsCode(SqlServiceCode.ERR_INACTIVE_TRANSACTION, e2);
            }
            var status = transaction.getTransactionStatus();
            assertFalse(status.isNormal());
            assertTrue(status.isError());
            assertEquals(SqlServiceCode.ERR_UNIQUE_CONSTRAINT_VIOLATION, status.getDiagnosticCode());
            assertNotNull(status.getLowSqlServiceException());
        }
    }

    @Test
    @Disabled // TODO remove Disabled
    void transactionStatus_afterCommit() throws Exception {
        var session = getSession();
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            transaction.commit(TgCommitType.DEFAULT);
            var status = transaction.getTransactionStatus();
            assertTrue(status.isNormal());
        }
    }

    @Test
    @Disabled // TODO remove Disabled
    void transactionStatus_afterRollback() throws Exception {
        var session = getSession();
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            transaction.rollback();
            var status = transaction.getTransactionStatus();
            assertTrue(status.isNormal());
        }
    }

    @Test
    void transactionStatus_afterClose() throws Exception {
        var session = getSession();
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            transaction.close();
            var e = assertThrowsExactly(TsurugiIOException.class, () -> {
                transaction.getTransactionStatus();
            });
            assertEqualsCode(IceaxeErrorCode.TX_ALREADY_CLOSED, e);
        }
    }

    @RepeatedTest(6)
    void doNothing() throws Exception {
        try (var session = DbTestConnector.createSession()) {
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                try (var tx = session.createTransaction(TgTxOption.ofOCC())) {
                    // do nothing
                }
            }
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

    private static void assertSelect(int expected, TsurugiSession session, TsurugiTransaction transaction) throws IOException, TsurugiTransactionException, InterruptedException {
        try (var ps = session.createQuery(SELECT_SQL)) {
            var list = transaction.executeAndGetList(ps);
            assertEquals(expected, list.size());
        }
    }
}
