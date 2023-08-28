package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * transaction read area test
 */
class DbTransactionReadAreaTest extends DbTestTableTester {

    private static final String TEST2 = "test2";
    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        dropTable(TEST2);
        createTest2Table();
        insertTest2Table(SIZE);

        logInitEnd(info);
    }

    private static void createTest2Table() throws IOException, InterruptedException {
        var sql = CREATE_TEST_SQL.replace(TEST, TEST2);
        executeDdl(getSession(), sql);
    }

    protected static void insertTest2Table(int size) throws IOException, InterruptedException {
        var sql = INSERT_SQL.replace(TEST, TEST2);

        var session = getSession();
        var tm = createTransactionManagerOcc(session, 3);
        try (var ps = session.createStatement(sql, INSERT_MAPPING)) {
            tm.execute(transaction -> {
                for (int i = 0; i < size; i++) {
                    var entity = createTestEntity(i);
                    transaction.executeAndGetCount(ps, entity);
                }
                return;
            });
        }
    }

    private static final TgTxOption LTX1 = TgTxOption.ofLTX(TEST).addInclusiveReadArea(TEST);
    private static final TgTxOption LTX2 = TgTxOption.ofLTX(TEST2).addInclusiveReadArea(TEST2);
    private static final TgTxOption LTX1_NOTHING = TgTxOption.ofLTX(TEST);
    private static final TgTxOption LTX2_NOTHING = TgTxOption.ofLTX(TEST2);

    private static final int KEY = 1;
    private static final String SELECT1_SQL = SELECT_SQL + " where foo = " + KEY;
    private static final String SELECT2_SQL = SELECT1_SQL.replace(TEST, TEST2);
    private static final String UPDATE1_SQL = "update " + TEST + " set bar=789 where foo = " + KEY;
    private static final String UPDATE2_SQL = UPDATE1_SQL.replace(TEST, TEST2);

    @Test
    void readArea1Select_nothing() throws Exception {
        var session = getSession();
        try (var select2Ps = session.createQuery(SELECT2_SQL, SELECT_MAPPING)) {
            try (var tx1 = session.createTransaction(LTX1_NOTHING)) {
                for (int i = 0; i < 3; i++) {
                    tx1.executeAndGetList(select2Ps);
                }
                tx1.commit(TgCommitType.DEFAULT);
            }
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void readArea1Select_error() throws Exception {
        var session = getSession();
        try (var select2Ps = session.createQuery(SELECT2_SQL, SELECT_MAPPING)) {
            try (var tx1 = session.createTransaction(LTX1)) {
                var e1 = assertThrowsExactly(TsurugiTransactionException.class, () -> tx1.executeAndGetList(select2Ps));
                assertEqualsCode(SqlServiceCode.ERR_ILLEGAL_OPERATION, e1);
//              assertContains("TODO", e1.getMessage()); // TODO エラーになったテーブルの確認
                var e2 = assertThrowsExactly(TsurugiTransactionException.class, () -> tx1.executeAndGetList(select2Ps));
                assertEqualsCode(SqlServiceCode.ERR_INACTIVE_TRANSACTION, e2);
            }
        }

        assertEqualsTestTable(SIZE);
    }

    @ParameterizedTest
    @ValueSource(strings = { "nothing", "read_area" })
    void readArea1Update(String pattern) throws Exception {
        var txOption = getTxOption(pattern);
        var session = getSession();
        try (var update1Ps = session.createStatement(UPDATE1_SQL)) {
            try (var tx1 = session.createTransaction(txOption)) {
                for (int i = 1; i < 3; i++) {
                    tx1.executeAndGetCount(update1Ps);
                }
                tx1.commit(TgCommitType.DEFAULT);
            }
        }

        assertTable(TEST);
    }

    @ParameterizedTest
    @ValueSource(strings = { "nothing", "read_area" })
    void readArea1Update_error(String pattern) throws Exception {
        var txOption = getTxOption(pattern);
        var session = getSession();
        try (var update2Ps = session.createStatement(UPDATE2_SQL)) {
            try (var tx1 = session.createTransaction(txOption)) {
                var e1 = assertThrowsExactly(TsurugiTransactionException.class, () -> tx1.executeAndGetCount(update2Ps));
                assertEqualsCode(SqlServiceCode.ERR_ILLEGAL_OPERATION, e1);
//              assertContains("TODO", e1.getMessage()); // TODO エラーになったテーブルの確認
                var e2 = assertThrowsExactly(TsurugiTransactionException.class, () -> tx1.executeAndGetCount(update2Ps));
                assertEqualsCode(SqlServiceCode.ERR_INACTIVE_TRANSACTION, e2);
            }
        }

        assertEqualsTestTable(SIZE);
    }

    private TgTxOption getTxOption(String pattern) {
        switch (pattern) {
        case "nothing":
            return LTX1_NOTHING;
        case "read_area":
            return LTX1;
        default:
            throw new AssertionError(pattern);
        }
    }

    @Test
    void readArea2_nothing() throws Exception {
        var session = getSession();
        try (var update1Ps = session.createStatement(UPDATE1_SQL); //
                var update2Ps = session.createStatement(UPDATE2_SQL)) {
            try (var tx1 = session.createTransaction(LTX1_NOTHING)) {
                tx1.executeAndGetCount(update1Ps);

                try (var tx2 = session.createTransaction(LTX2_NOTHING)) {
                    tx2.executeAndGetCount(update2Ps);

                    var future2 = executeFuture(() -> {
                        tx2.commit(TgCommitType.DEFAULT);
                        return null;
                    });

                    Thread.sleep(100);
                    assertFalse(future2.isDone());
                    tx1.commit(TgCommitType.DEFAULT);

                    future2.get();
                }
            }
        }

        assertTable(TEST);
        assertTable(TEST2);
    }

    @Test
    void readArea2() throws Exception {
        var session = getSession();
        try (var update1Ps = session.createStatement(UPDATE1_SQL); //
                var update2Ps = session.createStatement(UPDATE2_SQL)) {
            try (var tx1 = session.createTransaction(LTX1)) {
                tx1.executeAndGetCount(update1Ps);

                try (var tx2 = session.createTransaction(LTX2)) {
                    tx2.executeAndGetCount(update2Ps);
                    tx2.commit(TgCommitType.DEFAULT);

                    tx1.commit(TgCommitType.DEFAULT);
                }
            }
        }

        assertTable(TEST);
        assertTable(TEST2);
    }

    @Test
    void readArea2_1() throws Exception {
        var session = getSession();
        try (var select2Ps = session.createQuery(SELECT2_SQL); //
                var update1Ps = session.createStatement(UPDATE1_SQL); //
                var update2Ps = session.createStatement(UPDATE2_SQL)) {
            var tx1Option = TgTxOption.ofLTX(TEST).addInclusiveReadArea(TEST, TEST2);
            var tx2Option = TgTxOption.ofLTX(TEST2).addInclusiveReadArea(TEST2);
            try (var tx1 = session.createTransaction(tx1Option)) {
                tx1.executeAndGetList(select2Ps);
                tx1.executeAndGetCount(update1Ps);

                try (var tx2 = session.createTransaction(tx2Option)) {
                    tx2.executeAndGetCount(update2Ps);

                    var future2 = executeFuture(() -> {
                        tx2.commit(TgCommitType.DEFAULT);
                        return null;
                    });

                    Thread.sleep(100);
                    assertFalse(future2.isDone());
                    tx1.commit(TgCommitType.DEFAULT);

                    future2.get();
                }
            }
        }

        assertTable(TEST);
        assertTable(TEST2);
    }

    @Test
    void readArea2_2() throws Exception {
        var session = getSession();
        try (var select1Ps = session.createQuery(SELECT1_SQL); //
                var update1Ps = session.createStatement(UPDATE1_SQL); //
                var update2Ps = session.createStatement(UPDATE2_SQL)) {
            var tx1Option = TgTxOption.ofLTX(TEST).addInclusiveReadArea(TEST);
            var tx2Option = TgTxOption.ofLTX(TEST2).addInclusiveReadArea(TEST, TEST2);
            try (var tx1 = session.createTransaction(tx1Option)) {
                tx1.executeAndGetCount(update1Ps);

                try (var tx2 = session.createTransaction(tx2Option)) {
                    tx2.executeAndGetList(select1Ps);
                    tx2.executeAndGetCount(update2Ps);
                    tx2.commit(TgCommitType.DEFAULT);
                }

                tx1.commit(TgCommitType.DEFAULT);
            }
        }

        assertTable(TEST);
        assertTable(TEST2);
    }

    private static void assertTable(String tableName) throws IOException, InterruptedException {
        var session = getSession();
        try (var ps = session.createQuery(SELECT_SQL.replace(TEST, tableName), SELECT_MAPPING)) {
            var tm = createTransactionManagerOcc(session);
            var list = tm.executeAndGetList(ps);
            for (var entity : list) {
                if (entity.getFoo() == KEY) {
                    assertEquals(789L, entity.getBar());
                } else {
                    assertEquals(createTestEntity(entity.getFoo()), entity);
                }
            }
        }
    }
}
