package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * select error test
 */
class DbSelectErrorTest extends DbTestTableTester {

    private static final int SIZE = 100;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectErrorTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(LOG, info);
    }

    @Test
    void undefinedColumnName() throws Exception {
        var sql = "select hoge from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                tm.executeAndGetList(ps);
            });
            assertErrorVariableNotFound("hoge", e);
        }
    }

    @Test
    void aggregateWithoutGroupBy() throws Exception {
        var sql = "select foo, sum(bar) as bar from " + TEST; // without 'group by'

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                tm.executeAndGetList(ps);
            });
            assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e);
            assertContains("compile failed with error:invalid_aggregation_column message:\"column must be aggregated\" location:<input>:", e.getMessage()); // TODO カラム名が欲しい
        }
    }

    @Test
    void ps0ExecuteAfterClose() throws Exception {
        var sql = "select * from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var ps = session.createQuery(sql);
        ps.close();
        var e = assertThrowsExactly(IceaxeIOException.class, () -> {
            tm.executeAndGetList(ps);
        });
        assertEqualsCode(IceaxeErrorCode.PS_ALREADY_CLOSED, e);
    }

    @Test
    void ps1ExecuteAfterClose() throws Exception {
        var foo = TgBindVariable.ofInt("foo");
        var sql = "select * from " + TEST + " where foo=" + foo;
        var parameterMapping = TgParameterMapping.of(foo);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var ps = session.createQuery(sql, parameterMapping);
        ps.close();
        var parameter = TgBindParameters.of(foo.bind(1));
        var e = assertThrowsExactly(IceaxeIOException.class, () -> {
            tm.executeAndGetList(ps, parameter);
        });
        assertEqualsCode(IceaxeErrorCode.PS_ALREADY_CLOSED, e);
    }

    @Test
    void ps0ExecuteAfterTxFutureClose() throws Exception {
        ps0ExecuteAfterTxClose(false);
    }

    @Test
    void ps0ExecuteAfterTxClose() throws Exception {
        ps0ExecuteAfterTxClose(true);
    }

    private void ps0ExecuteAfterTxClose(boolean getLow) throws IOException, InterruptedException {
        var sql = "select * from " + TEST;

        var session = getSession();
        try (var ps = session.createQuery(sql)) {
            var transaction = session.createTransaction(TgTxOption.ofOCC());
            if (getLow) {
                transaction.getLowTransaction();
            }
            transaction.close();
            var e = assertThrowsExactly(IceaxeIOException.class, () -> {
                transaction.executeAndGetList(ps);
            });
            assertEqualsCode(IceaxeErrorCode.TX_ALREADY_CLOSED, e);
            assertEqualsMessage("transaction already closed", e);
        }
    }

    @Test
    void selectAfterTransactionClose() throws Exception {
        var sql = "select * from " + TEST;

        var session = getSession();
        try (var ps = session.createQuery(sql)) {
            TsurugiQueryResult<TsurugiResultEntity> result;
            try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                result = transaction.executeQuery(ps);
            }
            var e = assertThrowsExactly(IOException.class, () -> {
                result.getRecordList();
            });
            assertEqualsMessage("resultSet already closed", e);
        }
    }

    @Test
    void selectInTransactionClose() throws Exception {
        var sql = "select * from " + TEST;

        var session = getSession();
        try (var ps = session.createQuery(sql)) {
            try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                try (var result = transaction.executeQuery(ps)) {
                    var i = result.iterator();
                    i.next();
                    transaction.close();
                    var e = assertThrowsExactly(UncheckedIOException.class, () -> {
                        while (i.hasNext()) {
                            i.next();
                        }
                    });
                    assertEqualsMessage("resultSet already closed", e);
                }
            }
        }
    }

    @RepeatedTest(60)
    @Disabled // TODO remove Disabled. 挙動が不安定
    void closeWithSelectThread() throws Throwable {
        Thread thread;
        Throwable[] threadException = { null };
        try (var session = DbTestConnector.createSession()) {
            var started = new AtomicBoolean(false);
            var ended = new AtomicBoolean(false);
            thread = new Thread(() -> {
                try {
                    var setting = TgTmSetting.of(TgTxOption.ofOCC());
                    var tm = session.createTransactionManager(setting);
                    tm.execute(transaction -> {
                        try (var ps = session.createQuery(SELECT_SQL)) {
                            started.set(true);
                            transaction.executeAndGetList(ps);
                        }
                    });
                } catch (Throwable e) {
                    threadException[0] = e;
                } finally {
                    ended.set(true);
                }
            });
            thread.start();
            while (!started.get() && !ended.get()) {
            }
        } // session close (with resultSet, transaction, statement close)

        thread.join();

        Throwable e = threadException[0];
        assertNotNull(e);
        String message = e.getMessage();
        if (message != null) {
            try {
                assertContains("already closed", message);
            } catch (Throwable t) {
                t.addSuppressed(e);
                throw t;
            }
            return;
        }
        throw e;
    }
}
