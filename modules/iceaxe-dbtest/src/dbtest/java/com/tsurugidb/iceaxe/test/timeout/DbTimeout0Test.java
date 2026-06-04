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
package com.tsurugidb.iceaxe.test.timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * Timeout 0 test
 */
class DbTimeout0Test extends DbTestTableTester {

    private static final int SIZE = 2_000;

    // Verify that the process completes without timing out.
    @Test
    void timeout0() throws Exception {
        try (var session = DbTestConnector.createSession(0, TimeUnit.SECONDS)) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC(), 3);
            var tm = session.createTransactionManager(setting);
            tm.executeAndGetCount("drop table if exists " + TEST);
            tm.executeAndGetCount(CREATE_TEST_SQL);

            tm.execute(transaction -> {
                try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
                    for (int i = 0; i < SIZE; i++) {
                        var entity = createTestEntity(i);
                        transaction.executeAndGetCountDetail(ps, entity);
                    }
                }
                return;
            });

            var result = tm.executeAndGetList("select * from " + TEST + " order by foo");
            assertEquals(SIZE, result.size());
        }
    }
}
