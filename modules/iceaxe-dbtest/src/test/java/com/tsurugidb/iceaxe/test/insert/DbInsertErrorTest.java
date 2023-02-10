package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * insert error test
 */
class DbInsertErrorTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        if (!info.getDisplayName().equals("insertNullToNotNull()")) {
            createTestTable();
        }

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void insertNullToPK() throws IOException {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(null, 456, 'abc')";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            // TODO insert null to PK
            tm.executeAndGetCount(ps);
//          var e = assertThrowsExactly(TsurugiIOException.class, () -> tm.executeAndGetCount(ps));
//          assertEqualsCode(SqlServiceCode.ERR_INTEGRITY_CONSTRAINT_VIOLATION, e);
//          assertContains("TODO", e.getMessage()); // TODO エラー詳細情報の確認
        }

//      assertEqualsTestTable(0);
        var actualList = selectAllFromTest();
        assertEquals(1, actualList.size());
        var actual = actualList.get(0);
        assertEquals(null, actual.getFoo());
        assertEquals(456L, actual.getBar());
        assertEquals("abc", actual.getZzz());
    }

    @Test
    void insertNullToNotNull() throws IOException {
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
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                var entity = new TestEntity(123, null, null);
                tm.executeAndGetCount(ps, entity);
            });
            assertEqualsCode(SqlServiceCode.ERR_INTEGRITY_CONSTRAINT_VIOLATION, e);
            String expected = "ERR_INTEGRITY_CONSTRAINT_VIOLATION: SQL--0016: .";
            assertContains(expected, e.getMessage()); // TODO エラー詳細情報の確認
        }

        assertEqualsTestTable(0);
    }

    @Test
    void ps0ExecuteAfterClose() throws IOException {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(1, 1, '1')";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var ps = session.createPreparedStatement(sql);
        ps.close();
        var e = assertThrowsExactly(TsurugiIOException.class, () -> {
            tm.executeAndGetCount(ps);
        });
        assertEqualsCode(IceaxeErrorCode.PS_ALREADY_CLOSED, e);
    }

    @Test
    void ps1ExecuteAfterClose() throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING);
        ps.close();
        var entity = createTestEntity(1);
        var e = assertThrowsExactly(TsurugiIOException.class, () -> {
            tm.executeAndGetCount(ps, entity);
        });
        assertEqualsCode(IceaxeErrorCode.PS_ALREADY_CLOSED, e);
    }

    @Test
    void ps0ExecuteAfterTxFutureClose() throws IOException {
        ps0ExecuteAfterTxClose(false, "Future is already closed");
    }

    @Test
    void ps0ExecuteAfterTxClose() throws IOException {
        ps0ExecuteAfterTxClose(true, "already closed");
    }

    private void ps0ExecuteAfterTxClose(boolean getLow, String expected) throws IOException {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(1, 1, '1')";

        var session = getSession();
        try (var ps = session.createPreparedStatement(sql)) {
            var transaction = session.createTransaction(TgTxOption.ofOCC());
            if (getLow) {
                transaction.getLowTransaction();
            }
            transaction.close();
            var e = assertThrowsExactly(TsurugiIOException.class, () -> {
                transaction.executeAndGetCount(ps);
            });
            assertEqualsCode(IceaxeErrorCode.TX_ALREADY_CLOSED, e);
//          assertEquals(expected, e.getMessage());
        }
    }
}
