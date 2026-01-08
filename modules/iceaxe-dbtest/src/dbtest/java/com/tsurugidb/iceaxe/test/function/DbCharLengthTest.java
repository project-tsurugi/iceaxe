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

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * char_length function test
 */
class DbCharLengthTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbCharLengthTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTable();

        int i = 0;
        insert(i++, null);
        insert(i++, "");
        insert(i++, "abcdABCD");
        insert(i++, "ａｂｃＡＢＣやゆよゃゅょ");
        insert(i++, "\ud83d\ude0a\ud842\udfb7");

        logInitEnd(LOG, info);
    }

    private static void createTable() throws IOException, InterruptedException {
        String sql = "create table " + TEST + "(" //
                + "  pk int primary key," //
                + "  value varchar(40)" //
                + ")";
        executeDdl(getSession(), sql);
    }

    private static void insert(int pk, String value) throws IOException, InterruptedException {
        var session = getSession();
        var v = TgBindVariable.ofString("value");
        var insertSql = "insert or replace into " + TEST + " values(" + pk + ", " + v + ")";
        var insertMapping = TgParameterMapping.of(v);
        try (var ps = session.createStatement(insertSql, insertMapping)) {
            var tm = createTransactionManagerOcc(session);
            tm.execute(transaction -> {
                var parameter = TgBindParameters.of(v.bind(value));
                transaction.executeAndGetCount(ps, parameter);
            });
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "char_length", "character_length" })
    void test(String functionName) throws Exception {
        var sql = String.format("select value, %s(value) from " + TEST, functionName);

        var tm = createTransactionManagerOcc(getSession());
        tm.executeAndForEach(sql, entity -> {
            Integer expected = charLength(entity.getStringOrNull(0));
            Integer actual = entity.getIntOrNull(1);
            assertEquals(expected, actual);
        });
    }

    private Integer charLength(String s) {
        if (s == null) {
            return null;
        }

        return s.codePointCount(0, s.length());
    }
}
