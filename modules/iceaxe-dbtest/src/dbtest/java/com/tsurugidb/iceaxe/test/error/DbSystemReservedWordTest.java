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
package com.tsurugidb.iceaxe.test.error;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * system reserved word test
 *
 * <ul>
 * <li>アンダースコア2個で始まるテーブル名やカラム名はシステムで予約されており、ユーザーは使用することが出来ない。</li>
 * </ul>
 */
class DbSystemReservedWordTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();

        logInitEnd(info);
    }

    private static void executeErrorDdl(String sql) throws IOException, InterruptedException {
        var tm = createTransactionManagerOcc(getSession(), "executeErrorDdl", 1);
        tm.executeDdl(sql);
    }

    @Test
    void createTable() throws Exception {
        String tableName = "__test";
        dropTable(tableName);
        var sql = "create table " + tableName //
                + "(" //
                + "  foo int," //
                + "  bar bigint," //
                + "  zzz varchar(10)" //
                + ")";
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            executeErrorDdl(sql);
        });
        assertErrorSystemReservedWord(tableName, e);
    }

    @Test
    void createTableColumn() throws Exception {
        var sql = "create table " + TEST //
                + "(" //
                + "  __foo int," //
                + "  __bar bigint," //
                + "  __zzz varchar(10)" //
                + ")";
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            executeErrorDdl(sql);
        });
        assertErrorSystemReservedWord("__foo", e);
    }

    @Test
    void selectAs() throws Exception {
        int size = 4;
        createTestTable();
        insertTestTable(size);

        var sql = "select foo as __foo from " + TEST + " order by foo";
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                tm.executeAndGetList(ps);
            });
            assertErrorSystemReservedWord("__foo", e);
        }
    }

    private static void assertErrorSystemReservedWord(String expected, Exception actual) {
        assertEqualsCode(SqlServiceCode.SYNTAX_EXCEPTION, actual);
        assertContains("identifier must not start with two underscores: " + expected, actual.getMessage());
    }
}
