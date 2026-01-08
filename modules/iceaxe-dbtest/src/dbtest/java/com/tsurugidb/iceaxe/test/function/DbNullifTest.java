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
package com.tsurugidb.iceaxe.test.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * nullif function test
 */
class DbNullifTest extends DbTestTableTester {

    private static final int SIZE = 5;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(LOG, info);
    }

    @Test
    void test() throws Exception {
        var sql = "select foo, nullif(foo, 2) from " + TEST;

        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql);
        assertEquals(SIZE, list.size());

        for (var entity : list) {
            if (entity.getInt("foo") == 2) {
                assertNull(entity.getIntOrNull(1));
            } else {
                assertEquals(entity.getInt("foo"), entity.getInt(1));
            }
        }
    }

    @Test
    void testNull() throws Exception {
        assertNull(intFunction("nullif(null, 1)"));
        assertNull(intFunction("nullif(null, null)"));

        for (int i = -1; i <= 2; i++) {
            assertEquals(i, intFunction("nullif(" + i + ", null)"));
        }
    }

    private Integer intFunction(String function) throws IOException, InterruptedException {
        var sql = "select " + function + " from " + TEST + " limit 1";
        var resultMapping = TgResultMapping.ofSingle(Integer.class);

        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql, resultMapping);
        return list.get(0);
    }
}
