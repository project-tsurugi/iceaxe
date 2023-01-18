package com.tsurugidb.iceaxe.transaction.manager.event.counter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

class TgTmCountAtomicTest {

    @Test
    void test() {
        var count = new TgTmCountAtomic();
        init(count, 1, TgTmCountAtomic::incrementExecuteCount);
        init(count, 2, TgTmCountAtomic::incrementTransactionCount);
        init(count, 3, TgTmCountAtomic::incrementExceptionCount);
        init(count, 4, TgTmCountAtomic::incrementRetryCount);
        init(count, 5, TgTmCountAtomic::incrementRetryOverCount);
        init(count, 6, TgTmCountAtomic::incrementBeforeCommitCount);
        init(count, 7, TgTmCountAtomic::incrementCommitCount);
        init(count, 8, TgTmCountAtomic::incrementRollbackCount);
        init(count, 9, TgTmCountAtomic::incrementSuccessCommitCount);
        init(count, 10, TgTmCountAtomic::incrementSuccessRollbackCount);
        init(count, 11, TgTmCountAtomic::incrementFailCount);

        assertEquals(1, count.executeCount());
        assertEquals(2, count.transactionCount());
        assertEquals(3, count.execptionCount());
        assertEquals(4, count.retryCount());
        assertEquals(5, count.retryOverCount());
        assertEquals(6, count.beforeCommitCount());
        assertEquals(7, count.commitCount());
        assertEquals(8, count.rollbackCount());
        assertEquals(9, count.successCommitCount());
        assertEquals(10, count.successRollbackCount());
        assertEquals(9 + 10, count.successCount());
        assertEquals(11, count.failCount());

        assertEquals(
                "[executeCount=1, transactionCount=2, exceptionCount=3, retryCount=4, retryOverCount=5, beforeCommitCount=6, commitCount=7, rollbackCount=8, successCommitCount=9, successRollbackCount=10, failCount=11]",
                count.toString());

        count.clear();
        assertEquals(0, count.executeCount());
        assertEquals(0, count.transactionCount());
        assertEquals(0, count.execptionCount());
        assertEquals(0, count.retryCount());
        assertEquals(0, count.retryOverCount());
        assertEquals(0, count.beforeCommitCount());
        assertEquals(0, count.commitCount());
        assertEquals(0, count.rollbackCount());
        assertEquals(0, count.successCommitCount());
        assertEquals(0, count.successRollbackCount());
        assertEquals(0, count.successCount());
        assertEquals(0, count.failCount());
    }

    private static void init(TgTmCountAtomic count, int size, Consumer<TgTmCountAtomic> function) {
        for (int i = 0; i < size; i++) {
            function.accept(count);
        }
    }
}
