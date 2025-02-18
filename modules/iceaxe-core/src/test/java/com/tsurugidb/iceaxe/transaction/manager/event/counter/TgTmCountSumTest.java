/*
 * Copyright 2023-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.iceaxe.transaction.manager.event.counter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

class TgTmCountSumTest {

    @Test
    void test() {
        var count1 = new TgTmCountAtomic();
        init(count1, 1, TgTmCountAtomic::incrementExecuteCount);
        init(count1, 2, TgTmCountAtomic::incrementTransactionCount);
        init(count1, 3, TgTmCountAtomic::incrementExceptionCount);
        init(count1, 4, TgTmCountAtomic::incrementRetryCount);
        init(count1, 5, TgTmCountAtomic::incrementRetryOverCount);
        init(count1, 6, TgTmCountAtomic::incrementBeforeCommitCount);
        init(count1, 7, TgTmCountAtomic::incrementCommitCount);
        init(count1, 8, TgTmCountAtomic::incrementRollbackCount);
        init(count1, 9, TgTmCountAtomic::incrementSuccessCommitCount);
        init(count1, 10, TgTmCountAtomic::incrementSuccessRollbackCount);
        init(count1, 11, TgTmCountAtomic::incrementFailCount);
        var count2 = new TgTmCountAtomic();
        init(count2, 11, TgTmCountAtomic::incrementExecuteCount);
        init(count2, 12, TgTmCountAtomic::incrementTransactionCount);
        init(count2, 13, TgTmCountAtomic::incrementExceptionCount);
        init(count2, 14, TgTmCountAtomic::incrementRetryCount);
        init(count2, 15, TgTmCountAtomic::incrementRetryOverCount);
        init(count2, 16, TgTmCountAtomic::incrementBeforeCommitCount);
        init(count2, 17, TgTmCountAtomic::incrementCommitCount);
        init(count2, 18, TgTmCountAtomic::incrementRollbackCount);
        init(count2, 19, TgTmCountAtomic::incrementSuccessCommitCount);
        init(count2, 20, TgTmCountAtomic::incrementSuccessRollbackCount);
        init(count2, 21, TgTmCountAtomic::incrementFailCount);
        var count = TgTmCountSum.of(Stream.of(count1, count2));

        assertEquals(1 + 11, count.executeCount());
        assertEquals(2 + 12, count.transactionCount());
        assertEquals(3 + 13, count.exceptionCount());
        assertEquals(4 + 14, count.retryCount());
        assertEquals(5 + 15, count.retryOverCount());
        assertEquals(6 + 16, count.beforeCommitCount());
        assertEquals(7 + 17, count.commitCount());
        assertEquals(8 + 18, count.rollbackCount());
        assertEquals(9 + 19, count.successCommitCount());
        assertEquals(10 + 20, count.successRollbackCount());
        assertEquals(9 + 19 + 10 + 20, count.successCount());
        assertEquals(11 + 21, count.failCount());

        assertEquals(
                "[executeCount=12, transactionCount=14, exceptionCount=16, retryCount=18, retryOverCount=20, beforeCommitCount=22, commitCount=24, rollbackCount=26, successCommitCount=28, successRollbackCount=30, failCount=32]",
                count.toString());
    }

    private static void init(TgTmCountAtomic count, int size, Consumer<TgTmCountAtomic> function) {
        for (int i = 0; i < size; i++) {
            function.accept(count);
        }
    }
}
