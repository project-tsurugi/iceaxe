package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * insert constraint violation test
 */
public class DbInsertConstraintTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void insertSequentialTx() throws Exception {
        var entity = new TestEntity(123, 456, "abc");

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            int count1 = tm.executeAndGetCount(ps, entity);
            assertUpdateCount(1, count1);

            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                tm.executeAndGetCount(ps, entity);
            });
            assertEqualsCode(SqlServiceCode.ERR_ALREADY_EXISTS, e);
        }

        assertEqualsTestTable(entity);
    }

    @Test
    void insertSameTx() throws Exception {
        var entity = new TestEntity(123, 456, "abc");

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            var e0 = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                tm.execute((TsurugiTransactionAction) transaction -> {
                    int count1 = transaction.executeAndGetCount(ps, entity);
                    assertUpdateCount(1, count1);

                    var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                        transaction.executeAndGetCount(ps, entity);
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
    void insertSameTxIgnoreEx() throws Exception {
        var entity = new TestEntity(123, 456, "abc");

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            var e0 = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                tm.execute(transaction -> {
                    int count1 = transaction.executeAndGetCount(ps, entity);
                    assertUpdateCount(1, count1);

                    var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                        transaction.executeAndGetCount(ps, entity);
                    });
                    assertEqualsCode(SqlServiceCode.ERR_ALREADY_EXISTS, e);
                    // throw e; // ignore exception
                });
            });

            assertEqualsCode(SqlServiceCode.ERR_INACTIVE_TRANSACTION, e0);
        }

        assertEqualsTestTable();
    }

    @Test
    void insertSameTxLazyCheck() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            for (int i = 0; i < 10; i++) {
                insertSameTxLazyCheck(tm, ps);
            }
        }
    }

    private void insertSameTxLazyCheck(TsurugiTransactionManager tm, TsurugiSqlPreparedStatement<TestEntity> ps) throws IOException, InterruptedException {
        var e0 = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
            tm.execute((TsurugiTransactionAction) transaction -> {
                var entity = createTestEntity(1);
                var r1 = ps.execute(transaction, entity);
                var r2 = ps.execute(transaction, entity);
                try (r1; r2) {
                    int count1 = r1.getUpdateCount();
                    assertUpdateCount(1, count1);

                    var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                        r2.getUpdateCount();
                    });
                    assertEqualsCode(SqlServiceCode.ERR_ALREADY_EXISTS, e);
                    throw e;
                }
            });
        });
        assertEqualsCode(SqlServiceCode.ERR_ALREADY_EXISTS, e0);
//      assertContains("TODO", e0.getMessage()); // TODO エラー詳細情報の確認

        assertEqualsTestTable(0);
    }

    @Test
    void insertParallelTx() throws Exception {
        var entity = new TestEntity(123, 456, "abc");

        var session = getSession();
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            var e0 = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                try (var tx1 = session.createTransaction(TgTxOption.ofOCC()); //
                        var tx2 = session.createTransaction(TgTxOption.ofOCC())) {
                    int count1 = tx1.executeAndGetCount(ps, entity);
                    assertUpdateCount(1, count1);
                    int count2 = tx2.executeAndGetCount(ps, entity);
                    assertUpdateCount(1, count2);

                    tx1.commit(TgCommitType.DEFAULT);
                    var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                        tx2.commit(TgCommitType.DEFAULT);
                    });
                    assertEqualsCode(SqlServiceCode.ERR_ABORTED_RETRYABLE, e);

                    try (var tx3 = session.createTransaction(TgTxOption.ofOCC())) {
                        int count3 = tx3.executeAndGetCount(ps, entity);
                        assertUpdateCount(1, count3);
                        var e3 = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                            tx3.commit(TgCommitType.DEFAULT);
                        });
                        assertEqualsCode(SqlServiceCode.ERR_ALREADY_EXISTS, e3);
                        throw e3;
                    }
                }
            });

            assertEqualsCode(SqlServiceCode.ERR_ALREADY_EXISTS, e0);
        }

        assertEqualsTestTable(entity);
    }

    @Test
    void insertParallelTxRollback() throws Exception {
        var entity = new TestEntity(123, 456, "abc");

        var session = getSession();
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            try (var tx1 = session.createTransaction(TgTxOption.ofOCC()); //
                    var tx2 = session.createTransaction(TgTxOption.ofOCC())) {
                int count1 = tx1.executeAndGetCount(ps, entity);
                assertUpdateCount(1, count1);
                int count2 = tx2.executeAndGetCount(ps, entity);
                assertUpdateCount(1, count2);

                tx1.rollback();
                tx2.commit(TgCommitType.DEFAULT);
            }
        }

        assertEqualsTestTable(entity);
    }
}
