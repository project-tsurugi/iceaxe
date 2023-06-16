package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.manager.event.counter.TgTmCount;
import com.tsurugidb.iceaxe.transaction.manager.event.counter.TgTmLabelCounter;
import com.tsurugidb.iceaxe.transaction.manager.event.counter.TgTmSimpleCounter;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmRetryOverIOException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * {@link TsurugiTransactionManager} counter test
 */
class DbManagerCounterTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(3);

        logInitEnd(info);
    }

    @Test
    void simpleCounter() throws Exception {
        var session = getSession();
        var tm = session.createTransactionManager();

        var counter = new TgTmSimpleCounter();
        tm.addEventListener(counter);

        execute(tm);
        assertSimpleCounter(counter);
    }

    private void assertSimpleCounter(TgTmSimpleCounter counter) {
        var count = counter.getCount();
        assertTotalCount(count);
    }

    private void assertTotalCount(TgTmCount count) {
        assertEquals(count.successCount() + count.failCount(), count.executeCount());
        assertEquals(count.commitCount() + count.rollbackCount(), count.transactionCount());
        assertEquals(6, count.execptionCount());
        assertEquals(4, count.retryCount());
        assertEquals(1, count.retryOverCount());
        assertEquals(2, count.beforeCommitCount());
        assertEquals(2, count.commitCount());
        assertEquals(10, count.rollbackCount());
        assertEquals(2, count.successCommitCount());
        assertEquals(4, count.successRollbackCount());
        assertEquals(2, count.failCount());
    }

    @Test
    void labelCounter() throws Exception {
        var session = getSession();
        var tm = session.createTransactionManager();

        var counter = new TgTmLabelCounter();
        tm.addEventListener(counter);

        execute(tm);
        assertLabelCounter(counter);
    }

    private void assertLabelCounter(TgTmLabelCounter counter) {
        assertEquals(5, counter.getCountMap().size());

        {
            assertFalse(counter.findCount("zzz").isPresent());

            var tx1 = counter.findCount("tx1").get();
            assertEquals(tx1.successCount() + tx1.failCount(), tx1.executeCount());
            assertEquals(tx1.commitCount() + tx1.rollbackCount(), tx1.transactionCount());
            assertEquals(0, tx1.execptionCount());
            assertEquals(0, tx1.retryCount());
            assertEquals(0, tx1.retryOverCount());
            assertEquals(1, tx1.beforeCommitCount());
            assertEquals(1, tx1.commitCount());
            assertEquals(0, tx1.rollbackCount());
            assertEquals(1, tx1.successCommitCount());
            assertEquals(0, tx1.successRollbackCount());
            assertEquals(0, tx1.failCount());

            var tx2 = counter.findCount("tx2").get();
            assertEquals(tx2.successCount() + tx2.failCount(), tx2.executeCount());
            assertEquals(tx2.commitCount() + tx2.rollbackCount(), tx2.transactionCount());
            assertEquals(3, tx2.execptionCount());
            assertEquals(2, tx2.retryCount());
            assertEquals(1, tx2.retryOverCount());
            assertEquals(0, tx2.beforeCommitCount());
            assertEquals(0, tx2.commitCount());
            assertEquals(3, tx2.rollbackCount());
            assertEquals(0, tx2.successCommitCount());
            assertEquals(0, tx2.successRollbackCount());
            assertEquals(1, tx2.failCount());

            var tran1 = counter.findCount("tran1").get();
            assertEquals(tran1.successCount() + tran1.failCount(), tran1.executeCount());
            assertEquals(tran1.commitCount() + tran1.rollbackCount(), tran1.transactionCount());
            assertEquals(0, tran1.execptionCount());
            assertEquals(0, tran1.retryCount());
            assertEquals(0, tran1.retryOverCount());
            assertEquals(0, tran1.beforeCommitCount());
            assertEquals(0, tran1.commitCount());
            assertEquals(4, tran1.rollbackCount());
            assertEquals(0, tran1.successCommitCount());
            assertEquals(4, tran1.successRollbackCount());
            assertEquals(0, tran1.failCount());

            var tran2 = counter.findCount("tran2").get();
            assertEquals(tran2.successCount() + tran2.failCount(), tran2.executeCount());
            assertEquals(tran2.commitCount() + tran2.rollbackCount(), tran2.transactionCount());
            assertEquals(2, tran2.execptionCount());
            assertEquals(2, tran2.retryCount());
            assertEquals(0, tran2.retryOverCount());
            assertEquals(1, tran2.beforeCommitCount());
            assertEquals(1, tran2.commitCount());
            assertEquals(2, tran2.rollbackCount());
            assertEquals(1, tran2.successCommitCount());
            assertEquals(0, tran2.successRollbackCount());
            assertEquals(0, tran2.failCount());

            var empty = counter.findCount("").get();
            assertEquals(empty.successCount() + empty.failCount(), empty.executeCount());
            assertEquals(empty.commitCount() + empty.rollbackCount(), empty.transactionCount());
            assertEquals(1, empty.execptionCount());
            assertEquals(0, empty.retryCount());
            assertEquals(0, empty.retryOverCount());
            assertEquals(0, empty.beforeCommitCount());
            assertEquals(0, empty.commitCount());
            assertEquals(1, empty.rollbackCount());
            assertEquals(0, empty.successCommitCount());
            assertEquals(0, empty.successRollbackCount());
            assertEquals(1, empty.failCount());
        }
        {
            assertFalse(counter.findCountByPrefix("zzz").isPresent());

            var tx = counter.findCountByPrefix("tx").get();
            var tx1 = counter.findCount("tx1").get();
            var tx2 = counter.findCount("tx2").get();
            assertEquals(tx1.executeCount() + tx2.executeCount(), tx.executeCount());
            assertEquals(tx1.execptionCount() + tx2.execptionCount(), tx.execptionCount());
            assertEquals(tx1.retryCount() + tx2.retryCount(), tx.retryCount());
            assertEquals(tx1.retryOverCount() + tx2.retryOverCount(), tx.retryOverCount());
            assertEquals(tx1.beforeCommitCount() + tx2.beforeCommitCount(), tx.beforeCommitCount());
            assertEquals(tx1.commitCount() + tx2.commitCount(), tx.commitCount());
            assertEquals(tx1.rollbackCount() + tx2.rollbackCount(), tx.rollbackCount());
            assertEquals(tx1.successCommitCount() + tx2.successCommitCount(), tx.successCommitCount());
            assertEquals(tx1.successRollbackCount() + tx2.successRollbackCount(), tx.successRollbackCount());
            assertEquals(tx1.failCount() + tx2.failCount(), tx.failCount());
        }
        {
            var count = counter.getCount();
            assertTotalCount(count);
        }
    }

    @Test
    void mix() throws Exception {
        var session = getSession();
        var tm = session.createTransactionManager();

        var simpleCounter = new TgTmSimpleCounter();
        tm.addEventListener(simpleCounter);
        var labelCounter = new TgTmLabelCounter();
        tm.addEventListener(labelCounter);

        execute(tm);
        assertSimpleCounter(simpleCounter);
        assertLabelCounter(labelCounter);
    }

    private void execute(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var setting1 = TgTmSetting.of(TgTxOption.ofLTX(TEST).label("tx1"));
        var setting2 = TgTmSetting.ofAlways(TgTxOption.ofOCC().label("tx2"), 3);
        var setting3 = TgTmSetting.ofAlways(TgTxOption.ofOCC().label("tran1"));
        var setting4 = TgTmSetting.ofAlways(TgTxOption.ofOCC().label("tran2"));
        var setting5 = TgTmSetting.ofAlways(TgTxOption.ofOCC()); // label(null)

        var bar = TgBindVariable.ofInt("bar");
        var sql = "update " + TEST + " set bar=" + bar + " where foo=1";
        var mapping = TgParameterMapping.of(bar);

        var session = tm.getSession();
        try (var ps = session.createStatement(sql, mapping)) {
            tm.execute(setting1, tx1 -> {
                var param1 = TgBindParameters.of(bar.bind(111));
                tx1.executeAndGetCount(ps, param1);

                var e = assertThrowsExactly(TsurugiTmRetryOverIOException.class, () -> {
                    tm.execute(setting2, tx2 -> {
                        var param2 = TgBindParameters.of(bar.bind(222));
                        tx2.executeAndGetCount(ps, param2);
                    });
                });
                assertEqualsCode(SqlServiceCode.ERR_SERIALIZATION_FAILURE, e);

                for (int i = 0; i < 4; i++) {
                    tm.execute(setting3, tx3 -> {
                        tx3.rollback();
                    });
                }

                tm.execute(setting4, tx4 -> {
                    if (tx4.getAttempt() < 2) {
                        var param4 = TgBindParameters.of(bar.bind(333));
                        tx4.executeAndGetCount(ps, param4);
                    }
                });

                var e5 = assertThrowsExactly(RuntimeException.class, () -> {
                    tm.execute(setting5, (TsurugiTransactionAction) tx5 -> {
                        throw new RuntimeException("test5");
                    });
                });
                assertEquals("test5", e5.getMessage());
            });
        }
    }
}
