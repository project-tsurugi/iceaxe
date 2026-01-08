/*
 * Copyright 2023-2026 Project Tsurugi.
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
package com.tsurugidb.iceaxe.test.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.event.TsurugiTmEventListener;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOption;

/**
 * {@link TsurugiQueryResult} test
 */
class DbQueryResult2Test extends DbTestTableTester {

    private static final int SIZE = 10;
    private static final String SELECT_ORDER_BY_SQL = SELECT_SQL + " order by foo";

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(LOG, info);
    }

    @Test
    void whileEach_withRowNumber() throws Exception {
        var counter = new Object() {
            int i0 = 0; // i==0 が呼ばれた回数
            int count = 0; // 正しく読めた件数
            int total = 0; // リトライも含めて読んだ件数
            boolean updated = false;
            boolean retried = false;
        };

        var session = getSession();
        var tm = createTransactionManagerOcc(session, 2);
        tm.addEventListener(new TsurugiTmEventListener() {
            @Override
            public void transactionRetry(TsurugiTransaction transaction, Exception cause, TgTmTxOption nextTmOption) {
                counter.retried = true;
            }
        });

        try (var ps = session.createQuery(SELECT_ORDER_BY_SQL, SELECT_MAPPING)) {
            tm.execute(transaction -> {
                try (var result = transaction.executeQuery(ps)) {
                    assertEquals(Optional.empty(), result.getHasNextRow());

                    result.whileEach((i, entity) -> {
                        if (i == 0) {
                            counter.i0++;
                            counter.count = 0;
                        }
                        counter.count++;
                        counter.total++;

                        var expected = createTestEntity(i);
                        if (entity.getFoo() == 1 && counter.updated) {
                            expected.setBar(999L);
                        }
                        assertEquals(expected, entity);

                        if (i == SIZE / 2 && !counter.updated) {
                            // tmでシリアライゼーションエラーを発生させ、リトライさせる
                            var tm2 = createTransactionManagerOcc(session);
                            tm2.executeAndGetCount("update " + TEST + " set bar=999 where foo=1");
                            counter.updated = true;
                        }
                    });
                    assertEquals(Optional.of(false), result.getHasNextRow());
                    assertEquals(SIZE, result.getReadCount());
                }
            });

            assertTrue(counter.retried);
            assertEquals(2, counter.i0);
            assertEquals(SIZE, counter.count);
            assertTrue(counter.total > SIZE);
        }
    }
}
