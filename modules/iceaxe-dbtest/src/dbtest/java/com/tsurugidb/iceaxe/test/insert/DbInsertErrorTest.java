package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * insert error test
 */
class DbInsertErrorTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        if (!info.getDisplayName().equals("insertNullToNotNull()")) {
            createTestTable();
            insertTestTable(SIZE);
        }

        logInitEnd(info);
    }

    @Test
    void notFoundColumn() throws Exception {
        var sql = "insert into " + TEST + " (aaa) values(1)";

        var tm = createTransactionManagerOcc(getSession());
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            tm.executeAndGetCount(sql);
        });
        assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e);
        assertContains("compile failed with error:column_not_found message:\"column is not found: test.aaa\" location:<input>:", e.getMessage());
    }

    @Test
    void insertNullToPK() throws Exception {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(null, 456, 'abc')";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            var e0 = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                tm.execute((TsurugiTransactionAction) transaction -> {
                    var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                        transaction.executeAndGetCount(ps);
                    });
                    assertEqualsCode(SqlServiceCode.NOT_NULL_CONSTRAINT_VIOLATION_EXCEPTION, e);
                    assertContains("Null assigned for non-nullable field", e.getMessage()); // TODO エラー詳細情報（カラム名等）の確認
                    throw e;
                });
            });
            assertEqualsCode(SqlServiceCode.NOT_NULL_CONSTRAINT_VIOLATION_EXCEPTION, e0);
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void insertNullToNotNull() throws Exception {
        var session = getSession();

        var createSql = "create table " + TEST //
                + "(" //
                + "  foo int not null," //
                + "  bar bigint not null," //
                + "  zzz varchar(10) not null," //
                + "  primary key(foo)" //
                + ")";
        executeDdl(session, createSql);

        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            var e0 = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                tm.execute((TsurugiTransactionAction) transaction -> {
                    var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                        var entity = new TestEntity(123, null, null);
                        transaction.executeAndGetCount(ps, entity);
                    });
                    assertEqualsCode(SqlServiceCode.NOT_NULL_CONSTRAINT_VIOLATION_EXCEPTION, e);
                    assertContains("Null assigned for non-nullable field", e.getMessage()); // TODO エラー詳細情報（カラム名等）の確認
                    throw e;
                });
            });
            assertEqualsCode(SqlServiceCode.NOT_NULL_CONSTRAINT_VIOLATION_EXCEPTION, e0);
        }

        assertEqualsTestTable(0);
    }

    @Test
    void ps0ExecuteAfterClose() throws Exception {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(1, 1, '1')";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var ps = session.createStatement(sql);
        ps.close();
        var e = assertThrowsExactly(IceaxeIOException.class, () -> {
            tm.executeAndGetCount(ps);
        });
        assertEqualsCode(IceaxeErrorCode.PS_ALREADY_CLOSED, e);

        assertEqualsTestTable(SIZE);
    }

    @Test
    void ps1ExecuteAfterClose() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING);
        ps.close();
        var entity = createTestEntity(1);
        var e = assertThrowsExactly(IceaxeIOException.class, () -> {
            tm.executeAndGetCount(ps, entity);
        });
        assertEqualsCode(IceaxeErrorCode.PS_ALREADY_CLOSED, e);

        assertEqualsTestTable(SIZE);
    }

    @Test
    void ps0ExecuteAfterTxFutureClose() throws Exception {
        ps0ExecuteAfterTxClose(false, "Future is already closed");
    }

    @Test
    void ps0ExecuteAfterTxClose() throws Exception {
        ps0ExecuteAfterTxClose(true, "already closed");
    }

    private void ps0ExecuteAfterTxClose(boolean getLow, String expected) throws IOException, InterruptedException {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(1, 1, '1')";

        var session = getSession();
        try (var ps = session.createStatement(sql)) {
            var transaction = session.createTransaction(TgTxOption.ofOCC());
            if (getLow) {
                transaction.getLowTransaction();
            }
            transaction.close();
            var e = assertThrowsExactly(IceaxeIOException.class, () -> {
                transaction.executeAndGetCount(ps);
            });
            assertEqualsCode(IceaxeErrorCode.TX_ALREADY_CLOSED, e);
        }

        assertEqualsTestTable(SIZE);
    }
}
