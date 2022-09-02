package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * insert constraint violation test
 */
public class DbInsertConstraintTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void insertSequentialTx() throws IOException {
        var entity = new TestEntity(123, 456, "abc");

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            int count1 = ps.executeAndGetCount(tm, entity);
            assertEquals(-1, count1); // TODO 1

            var e = assertThrows(TsurugiTransactionIOException.class, () -> {
                ps.executeAndGetCount(tm, entity);
            });
            assertEqualsCode(SqlServiceCode.ERR_ALREADY_EXISTS, e);
        }

        assertEqualsTestTable(entity);
    }

    @Test
    void insertSameTx() throws IOException {
        var entity = new TestEntity(123, 456, "abc");

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            var e0 = assertThrows(TsurugiTransactionIOException.class, () -> {
                tm.execute((TsurugiTransactionAction) transaction -> {
                    int count1 = ps.executeAndGetCount(transaction, entity);
                    assertEquals(-1, count1); // TODO 1

                    var e = assertThrows(TsurugiTransactionException.class, () -> {
                        ps.executeAndGetCount(transaction, entity);
                    });
                    assertEqualsCode(SqlServiceCode.ERR_ALREADY_EXISTS, e);
                    throw e;
                });
            });

            assertEqualsCode(SqlServiceCode.ERR_ALREADY_EXISTS, e0);
        }

        assertEqualsTestTable();
    }

    @Test
    void insertSameTxIgnoreEx() throws IOException {
        var entity = new TestEntity(123, 456, "abc");

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            var e0 = assertThrows(TsurugiTransactionIOException.class, () -> {
                tm.execute(transaction -> {
                    int count1 = ps.executeAndGetCount(transaction, entity);
                    assertEquals(-1, count1); // TODO 1

                    var e = assertThrows(TsurugiTransactionException.class, () -> {
                        ps.executeAndGetCount(transaction, entity);
                    });
                    assertEqualsCode(SqlServiceCode.ERR_ALREADY_EXISTS, e);
                    // throw e; // ignore exception
                });
            });

            assertEqualsCode(SqlServiceCode.ERR_UNKNOWN, e0);// TODO ERR_INACTIVE_TRANSACTION
        }

        assertEqualsTestTable();
    }

    @Test
    void insertParallelTx() throws IOException {
        var entity = new TestEntity(123, 456, "abc");

        var session = getSession();
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            var e0 = assertThrows(TsurugiTransactionException.class, () -> {
                try (var tx1 = session.createTransaction(TgTxOption.ofOCC()); //
                        var tx2 = session.createTransaction(TgTxOption.ofOCC())) {
                    int count1 = ps.executeAndGetCount(tx1, entity);
                    assertEquals(-1, count1); // TODO 1
                    int count2 = ps.executeAndGetCount(tx2, entity);
                    assertEquals(-1, count2); // TODO 1

                    tx1.commit(TgCommitType.DEFAULT);
                    var e = assertThrows(TsurugiTransactionException.class, () -> {
                        tx2.commit(TgCommitType.DEFAULT);
                    });
                    assertEqualsCode(SqlServiceCode.ERR_ABORTED, e); // TODO ERR_ALREADY_EXISTS
                    throw e;
                }
            });

            assertEqualsCode(SqlServiceCode.ERR_ABORTED, e0);
        }

        assertEqualsTestTable(entity);
    }

    @Test
    void insertParallelTxRollback() throws IOException {
        var entity = new TestEntity(123, 456, "abc");

        var session = getSession();
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            var e0 = assertThrows(TsurugiTransactionException.class, () -> {
                try (var tx1 = session.createTransaction(TgTxOption.ofOCC()); //
                        var tx2 = session.createTransaction(TgTxOption.ofOCC())) {
                    int count1 = ps.executeAndGetCount(tx1, entity);
                    assertEquals(-1, count1); // TODO 1
                    int count2 = ps.executeAndGetCount(tx2, entity);
                    assertEquals(-1, count2); // TODO 1

                    tx1.rollback();
                    var e = assertThrows(TsurugiTransactionException.class, () -> {
                        tx2.commit(TgCommitType.DEFAULT);
                    });
                    assertEqualsCode(SqlServiceCode.ERR_ABORTED, e); // TODO ERR_ALREADY_EXISTS ?
                    throw e;
                }
            });

            assertEqualsCode(SqlServiceCode.ERR_ABORTED, e0);
        }

        assertEqualsTestTable();
    }
}
