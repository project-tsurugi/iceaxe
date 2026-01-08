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

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * statement comment test
 */
class DbStatementCommentTest extends DbTestTableTester {

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void comment(boolean prepared) throws Exception {
        var list1 = List.of("", "-- comment1\n", "/*comment1*/");
        var list2 = List.of("", "-- comment2\n", "/*comment2*/");
        var list3 = List.of("", "-- comment3", "-- comment3\n", "/*comment3*/");
        for (var c1 : list1) {
            for (var c2 : list2) {
                for (var c3 : list3) {
                    dropTestTable();
                    createTestTable();

                    var sql = c1 //
                            + "insert into " + TEST + "(foo, bar, zzz)\n" //
                            + c2 //
                            + "values(:foo, :bar, :zzz)\n" //
                            + c3;
                    try {
                        test(sql, prepared);
                    } catch (Throwable e) {
                        LOG.error("sql=[{}]", sql, e);
                        throw e;
                    }
                }
            }
        }
    }

    private void test(String sql, boolean prepared) throws IOException, InterruptedException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var entity = createTestEntity(0);
        if (prepared) {
            try (var ps = session.createStatement(sql, INSERT_MAPPING)) {
                int count = tm.executeAndGetCount(ps, entity);
                assertUpdateCount(1, count);
            }
        } else {
            var sqlr = sql.replace(":foo", Integer.toString(entity.getFoo())) //
                    .replace(":bar", Long.toString(entity.getBar())) //
                    .replace(":zzz", "'" + entity.getZzz() + "'");
            try (var ps = session.createStatement(sqlr)) {
                int count = tm.executeAndGetCount(ps);
                assertUpdateCount(1, count);
            }
        }
        assertEqualsTestTable(1);
    }
}
