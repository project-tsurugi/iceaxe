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
package com.tsurugidb.iceaxe.test.update;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;

/**
 * update long test
 */
class DbUpdateLongTest extends DbTestTableTester {

    private final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTable();
        insertTable(SIZE);

        logInitEnd(info);
    }

    private static void createTable() throws IOException, InterruptedException {
        var sql = "create table " + TEST //
                + "(" //
                + "  pk int primary key," //
                + "  int_value int," //
                + "  long_value bigint" //
                + ")";
        executeDdl(getSession(), sql);
    }

    private static void insertTable(int size) throws IOException, InterruptedException {
        var pk = TgBindVariable.ofInt("pk");
        var intValue = TgBindVariable.ofInt("intValue");
        var longValue = TgBindVariable.ofLong("longValue");
        var variables = TgBindVariables.of(pk, intValue, longValue);
        var sql = "insert into " + TEST + " values(" + variables.getSqlNames() + ")";
        var mapping = TgParameterMapping.of(variables);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql, mapping)) {
            tm.execute((TsurugiTransactionAction) transaction -> {
                for (int i = 0; i < size; i++) {
                    var parameter = TgBindParameters.of(pk.bind(i), intValue.bind(10 + i), longValue.bind(100 + i));
                    transaction.executeAndGetCount(ps, parameter);
                }
            });
        }
    }

    @Test
    void updateLongFromInt() throws Exception {
        var sql = "update " + TEST + " set long_value = int_value";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            tm.executeAndGetCount(ps);
        }

        var list = tm.executeAndGetList("select * from " + TEST + " order by pk");
        assertEquals(SIZE, list.size());
        for (var entity : list) {
            assertEquals(entity.getLong("int_value"), entity.getLong("long_value"));
        }
    }

    @Test
    void updateIntFromLong() throws Exception {
        var sql = "update " + TEST + " set int_value = long_value";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            tm.executeAndGetCount(ps);
        }

        var list = tm.executeAndGetList("select * from " + TEST + " order by pk");
        assertEquals(SIZE, list.size());
        for (var entity : list) {
            assertEquals(entity.getLong("long_value"), entity.getLong("int_value"));
        }
    }
}
