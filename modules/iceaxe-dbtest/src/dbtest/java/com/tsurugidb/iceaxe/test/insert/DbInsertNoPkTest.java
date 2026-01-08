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
package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;

/**
 * insert (table without primary key) test
 */
class DbInsertNoPkTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTable();

        logInitEnd(info);
    }

    private static void createTable() throws IOException, InterruptedException {
        // no primary key
        var sql = "create table " + TEST //
                + "(" //
                + "  foo int," //
                + "  bar bigint," //
                + "  zzz varchar(10)" //
                + ")";
        executeDdl(getSession(), sql);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void insertConstant(boolean columns) throws Exception {
        new DbInsertTest().insertConstant(columns);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void insertByBindVariables(boolean columns) throws Exception {
        new DbInsertTest().insertByBindVariables(columns);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void insertByBind(boolean columns) throws Exception {
        new DbInsertTest().insertByBind(columns);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void insertByEntityMapping(boolean columns) throws Exception {
        new DbInsertTest().insertByEntityMapping(columns);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void insertByEntityConverter(boolean columns) throws Exception {
        new DbInsertTest().insertByEntityConverter(columns);
    }

    @Test
    void insertMany() throws Exception {
        new DbInsertTest().insertMany();
    }

    @Test
    void insertResultCheck() throws Exception {
        new DbInsertTest().insertResultCheck();
    }

    @Test
    void insertResultNoCheck() throws Exception {
        new DbInsertTest().insertResultNoCheck();
    }

    @Test
    void insertDuplicate() throws Exception {
        var entity = new TestEntity(123, 456, "abc");
        int size = 4;

        var sql = "insert into " + TEST //
                + " (" + TEST_COLUMNS + ")" //
                + " values(" + entity.getFoo() + ", " + entity.getBar() + ", '" + entity.getZzz() + "')";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            for (int i = 0; i < size; i++) {
                tm.execute(transaction -> {
                    int count = transaction.executeAndGetCount(ps);
                    assertUpdateCount(1, count);
                });
            }
        }

        var actualList = selectAllFromTest();
        assertEquals(size, actualList.size());
        for (var actual : actualList) {
            assertEquals(entity, actual);
        }
    }
}
