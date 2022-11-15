package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.result.TsurugiResultSet;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * select error test
 */
class DbSelectErrorTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll() throws IOException {
        var LOG = LoggerFactory.getLogger(DbSelectErrorTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("init end");
    }

    @Test
    void undefinedColumnName() throws IOException {
        var sql = "select hoge from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                ps.executeAndGetList(tm);
            });
            assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
            assertContains("TODO", e.getMessage()); // TODO エラー詳細情報の確認
        }
    }

    @Test
    void aggregateWithoutGroupBy() throws IOException {
        var sql = "select foo, sum(bar) as bar from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                ps.executeAndGetList(tm);
            });
            assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
            assertContains("TODO", e.getMessage()); // TODO エラー詳細情報の確認
        }
    }

    @Test
    void ps0ExecuteAfterClose() throws IOException {
        var sql = "select * from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var ps = session.createPreparedQuery(sql);
        ps.close();
        var e = assertThrowsExactly(TsurugiIOException.class, () -> {
            ps.executeAndGetList(tm);
        });
        assertEqualsCode(IceaxeErrorCode.PS_ALREADY_CLOSED, e);
    }

    @Test
    void ps1ExecuteAfterClose() throws IOException {
        var foo = TgVariable.ofInt4("foo");
        var sql = "select * from " + TEST + " where foo=" + foo;
        var parameterMapping = TgParameterMapping.of(foo);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var ps = session.createPreparedQuery(sql, parameterMapping);
        ps.close();
        var parameter = TgParameterList.of(foo.bind(1));
        var e = assertThrowsExactly(TsurugiIOException.class, () -> {
            ps.executeAndGetList(tm, parameter);
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
        var sql = "select * from " + TEST;

        var session = getSession();
        try (var ps = session.createPreparedQuery(sql)) {
            var transaction = session.createTransaction(TgTxOption.ofOCC());
            if (getLow) {
                transaction.getLowTransaction();
            }
            transaction.close();
            var e = assertThrowsExactly(TsurugiIOException.class, () -> {
                ps.executeAndGetList(transaction);
            });
            assertEqualsCode(IceaxeErrorCode.TX_ALREADY_CLOSED, e);
//          assertEquals(expected, e.getMessage());
        }
    }

    @Test
    @Disabled // TODO remove @Disabled このテストの実行後にdrop tableでtateyama-serverが落ちることがある
    void selectAfterTransactionClose() throws IOException, TsurugiTransactionException {
        var sql = "select * from " + TEST;

        var session = getSession();
        try (var ps = session.createPreparedQuery(sql)) {
            TsurugiResultSet<TsurugiResultEntity> rs;
            try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                rs = ps.execute(transaction);
            }
            var e = assertThrowsExactly(IOException.class, () -> {
                rs.getRecordList();
            });
            assertMatches("Future .+ is already closed", e.getMessage());
        }
    }

    @Test
    void selectInTransactionClose() throws IOException, TsurugiTransactionException {
        var sql = "select * from " + TEST;

        var session = getSession();
        try (var ps = session.createPreparedQuery(sql)) {
            try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                try (var rs = ps.execute(transaction)) {
                    var i = rs.iterator();
                    i.next();
                    transaction.close();
                    while (i.hasNext()) {
                        i.next();
                    }
                }
            }
        }
    }
}
