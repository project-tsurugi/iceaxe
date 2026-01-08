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
package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;

/**
 * select all record test
 */
class DbSelectTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(LOG, info);
    }

    @Test
    void selectAllByResultEntity() throws Exception {
        var sql = "select * from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            List<TsurugiResultEntity> list = tm.executeAndGetList(ps);
            assertResultEntity(list);
        }
    }

    private static void assertResultEntity(List<TsurugiResultEntity> actualList) {
        assertEquals(SIZE, actualList.size());
        var actualMap = actualList.stream().collect(Collectors.toMap(r -> r.getIntOrNull("foo"), r -> r));
        for (int i = 0; i < SIZE; i++) {
            var actual = actualMap.get(i);
            var expected = createTestEntity(i);
            assertEquals(expected.getFoo(), actual.getIntOrNull("foo"));
            assertEquals(expected.getBar(), actual.getLongOrNull("bar"));
            assertEquals(expected.getZzz(), actual.getStringOrNull("zzz"));

            assertEquals(List.of("foo", "bar", "zzz"), actual.getNameList());
            assertEquals("foo", actual.getName(0));
            assertEquals("bar", actual.getName(1));
            assertEquals("zzz", actual.getName(2));
            assertThrows(IndexOutOfBoundsException.class, () -> actual.getName(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> actual.getName(3));
        }
    }

    @Test
    void selectAllByTestEntity() throws Exception {
        var sql = "select * from " + TEST;
        var resultMapping = TgResultMapping.of(TestEntity::new) //
                .addInt("foo", TestEntity::setFoo) //
                .addLong("bar", TestEntity::setBar) //
                .addString("zzz", TestEntity::setZzz);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, resultMapping)) {
            List<TestEntity> list = tm.executeAndGetList(ps);
            assertTestEntity(list);
        }
    }

    @Test
    void selectColumnsByTestEntity() throws Exception {
        var sql = "select " + TEST_COLUMNS + " from " + TEST;
        var resultMapping = TgResultMapping.of(TestEntity::new) //
                .addInt(TestEntity::setFoo) //
                .addLong(TestEntity::setBar) //
                .addString(TestEntity::setZzz);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, resultMapping)) {
            List<TestEntity> list = tm.executeAndGetList(ps);
            assertTestEntity(list);
        }
    }

    private static void assertTestEntity(List<TestEntity> actualList) {
        assertEquals(SIZE, actualList.size());
        var actualMap = actualList.stream().collect(Collectors.toMap(r -> r.getFoo(), r -> r));
        for (int i = 0; i < SIZE; i++) {
            var actual = actualMap.get(i);
            var expected = createTestEntity(i);
            assertEquals(expected, actual);
        }
    }

    @Test
    void selectColumn_convert() throws Exception {
        var sql = "select foo from " + TEST;
        var resultMapping = TgResultMapping.of(record -> record.nextInt());

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, resultMapping)) {
            List<Integer> list = tm.executeAndGetList(ps);
            assertColumn(list);
        }
    }

    @Test
    void selectColumn_single() throws Exception {
        var sql = "select foo from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(int.class);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, resultMapping)) {
            List<Integer> list = tm.executeAndGetList(ps);
            assertColumn(list);
        }
    }

    @Test
    void selectMultiColumn_single() throws Exception {
        var sql = "select foo, bar, zzz from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(int.class);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, resultMapping)) {
            List<Integer> list = tm.executeAndGetList(ps);
            assertColumn(list);
        }
    }

    private static void assertColumn(List<Integer> actualList) {
        assertEquals(SIZE, actualList.size());
        for (int i = 0; i < SIZE; i++) {
            assertTrue(actualList.contains(i));
        }
    }
}
