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

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * PreparedStatement execute error test
 */
class DbPsExecuteErrorTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(info);
    }

    @Test
    void intsertByExecuteQuery() throws Exception {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(123, 456, 'abc')";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                tm.executeAndGetList(ps);
            });
            assertEqualsCode(SqlServiceCode.INCONSISTENT_STATEMENT_EXCEPTION, e);
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void selectByExecuteStatement() throws Exception {
        var sql = "select * from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            int count = tm.executeAndGetCount(ps);
            assertUpdateCount(0, count); // TODO 0?
        }
    }

    @Test
    void insertParameterSizeUnmatch() throws Exception {
        var foo = TgBindVariable.ofInt("foo");
        var bar = TgBindVariable.ofLong("bar");
        var zzz = TgBindVariable.ofString("zzz");
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(" + foo + ", " + bar + ", " + zzz + ")";
        var parameterMapping = TgParameterMapping.of(foo, bar, zzz);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql, parameterMapping)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                var parameter = TgBindParameters.of(foo.bind(123), bar.bind(456) /* ,zzz */);
                tm.executeAndGetCount(ps, parameter);
            });
            assertEqualsCode(SqlServiceCode.UNRESOLVED_PLACEHOLDER_EXCEPTION, e);
            assertContains("Value is not assigned for host variable 'zzz'.", e.getMessage());
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void insertParameterTypeMismatch() throws Exception {
        var foo = TgBindVariable.ofLong("foo"); // INT4 <-> Int8
        var bar = TgBindVariable.ofInt("bar"); // INT8 <-> Int4
        var zzz = TgBindVariable.ofInt("zzzi"); // CHARACTER <-> Int4
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(" + foo + ", " + bar + ", " + zzz + ")";
        var parameterMapping = TgParameterMapping.of(foo, bar, zzz);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql, parameterMapping)) {
            var e = assertThrowsExactly(TsurugiIOException.class, () -> {
                var parameter = TgBindParameters.of(foo.bind(123), bar.bind(456), zzz.bind(789));
                tm.executeAndGetCount(ps, parameter);
            });
            assertEqualsCode(SqlServiceCode.TYPE_ANALYZE_EXCEPTION, e);
            assertContains("compile failed with error:inconsistent_type message:\"int4() (expected: {character_string})\" location:<input>:", e.getMessage()); // TODO カラム名の確認
        }

        assertEqualsTestTable(SIZE);
    }
}
