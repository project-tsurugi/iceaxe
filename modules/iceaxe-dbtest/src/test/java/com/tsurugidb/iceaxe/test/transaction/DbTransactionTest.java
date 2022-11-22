package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ResponseBox;

/**
 * transaction test
 */
class DbTransactionTest extends DbTestTableTester {

    private static final int SIZE = 2;
    private static final int ATTEMPT_SIZE = ResponseBox.responseBoxSize() + 100;

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("{} init end", info.getDisplayName());
    }

    @RepeatedTest(6)
    void doNothing() throws IOException {
        try (var session = DbTestConnector.createSession()) {
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                try (var tx = session.createTransaction(TgTxOption.ofOCC())) {
                    // do nothing
                }
            }
        }
    }

    @Test
    void commit() throws IOException, TsurugiTransactionException {
        var session = getSession();
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            assertSelect(SIZE, session, transaction);

            var entity = createTestEntity(SIZE);
            ps.executeAndGetCount(transaction, entity);

            assertSelect(SIZE + 1, session, transaction);

            transaction.commit(TgCommitType.DEFAULT);
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void commitTm() throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            tm.execute(transaction -> {
                assertSelect(SIZE, session, transaction);

                var entity = createTestEntity(SIZE);
                ps.executeAndGetCount(transaction, entity);

                assertSelect(SIZE + 1, session, transaction);
            });
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void rollback() throws IOException, TsurugiTransactionException {
        var session = getSession();
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            assertSelect(SIZE, session, transaction);

            var entity = createTestEntity(SIZE);
            ps.executeAndGetCount(transaction, entity);

            assertSelect(SIZE + 1, session, transaction);

            transaction.rollback();
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void rollbackTmByException() throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            assertThrowsExactly(IOException.class, () -> {
                tm.execute((TsurugiTransactionAction) transaction -> {
                    assertSelect(SIZE, session, transaction);

                    var entity = createTestEntity(SIZE);
                    ps.executeAndGetCount(transaction, entity);

                    assertSelect(SIZE + 1, session, transaction);

                    throw new IOException("test");
                });
            });
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void rollbackTmExplicit() throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            tm.execute(transaction -> {
                assertSelect(SIZE, session, transaction);

                var entity = createTestEntity(SIZE);
                ps.executeAndGetCount(transaction, entity);

                assertSelect(SIZE + 1, session, transaction);

                transaction.rollback();
            });
        }

        assertEqualsTestTable(SIZE);
    }

    private static void assertSelect(int expected, TsurugiSession session, TsurugiTransaction transaction) throws IOException, TsurugiTransactionException {
        try (var ps = session.createPreparedQuery(SELECT_SQL)) {
            var list = ps.executeAndGetList(transaction);
            assertEquals(expected, list.size());
        }
    }
}
