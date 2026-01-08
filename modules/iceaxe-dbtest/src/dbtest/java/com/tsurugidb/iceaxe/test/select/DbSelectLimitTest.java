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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;

/**
 * select limit test
 */
class DbSelectLimitTest extends DbTestTableTester {

    private static final int SIZE = 40;
    private static final List<TestEntity> TEST_LIST;
    static {
        var list = new ArrayList<TestEntity>();
        for (int i = 0; i < SIZE; i++) {
            var entity = new TestEntity(i, i ^ 3, Integer.toString(i / 3));
            list.add(entity);
        }
        TEST_LIST = list;
    }

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectLimitTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable(false);
        insertTestTable(TEST_LIST);

        logInitEnd(LOG, info);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 10, SIZE - 1, SIZE, SIZE + 1 })
    void limit(int limit) throws Exception {
        limit(limit, false);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 10, SIZE - 1, SIZE, SIZE + 1 })
    void orderBy_limit(int limit) throws Exception {
        limit(limit, true);
    }

    private void limit(int limit, boolean order) throws Exception {
        var sql = "select * from " + TEST //
                + (order ? " order by bar" : "") //
                + " limit " + limit;
        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql, SELECT_MAPPING);

        if (order) {
            var expectedList = TEST_LIST.stream() //
                    .sorted(Comparator.comparing(TestEntity::getBar)) //
                    .limit(limit) //
                    .collect(Collectors.toList());
            assertEquals(expectedList, list);
        } else {
            int expectedSize = Math.min(limit, SIZE);
            assertEquals(expectedSize, list.size());
            var set = new HashSet<Integer>();
            for (var actual : list) {
                assertTrue(set.add(actual.getFoo()));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 10, SIZE / 2 - 1, SIZE / 2, SIZE / 2 + 1 })
    void where_limit(int limit) throws Exception {
        where_limit(limit, false);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 10, SIZE / 2 - 1, SIZE / 2, SIZE / 2 + 1 })
    void where_orderBy_limit(int limit) throws Exception {
        where_limit(limit, true);
    }

    private void where_limit(int limit, boolean order) throws Exception {
        var sql = "select * from " + TEST //
                + " where bar % 2 = 0" //
                + (order ? " order by bar" : "") //
                + " limit " + limit;
        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql, SELECT_MAPPING);

        if (order) {
            var expectedList = TEST_LIST.stream() //
                    .filter(entity -> entity.getBar() % 2 == 0) //
                    .sorted(Comparator.comparing(TestEntity::getBar)) //
                    .limit(limit) //
                    .collect(Collectors.toList());
            assertEquals(expectedList, list);
        } else {
            int expectedSize = Math.min(limit, SIZE / 2);
            assertEquals(expectedSize, list.size());
            for (var entity : list) {
                long bar = entity.getBar();
                assertEquals(0, bar % 2);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, SIZE / 3 - 1, SIZE / 3, SIZE / 3 + 1 })
    void groupBy_limit(int limit) throws Exception {
        groupBy_limit(limit, false);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, SIZE / 3 - 1, SIZE / 3, SIZE / 3 + 1 })
    void groupBy_orderBy_limit(int limit) throws Exception {
        groupBy_limit(limit, true);
    }

    private void groupBy_limit(int limit, boolean order) throws Exception {
        var sql = "select zzz, count(*) cnt from " + TEST //
                + " group by zzz" //
                + (order ? " order by zzz" : "") //
                + " limit " + limit;
        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql);

        var expectedAllMap = TEST_LIST.stream().collect(Collectors.groupingBy(TestEntity::getZzz, Collectors.counting()));
        if (order) {
            var expectedList = expectedAllMap.entrySet().stream() //
                    .sorted(Comparator.comparing(entry -> entry.getKey())) //
                    .limit(limit) //
                    .collect(Collectors.toList());
            assertEquals(expectedList.size(), list.size());
            int i = 0;
            for (var actual : list) {
                var expected = expectedList.get(i++);
                assertEquals(expected.getKey(), actual.getString("zzz"));
                assertEquals(expected.getValue(), actual.getLong("cnt"));
            }
        } else {
            int expectedSize = Math.min(limit, expectedAllMap.size());
            assertEquals(expectedSize, list.size());
            for (var actual : list) {
                String zzz = actual.getString("zzz");
                assertEquals(expectedAllMap.get(zzz), actual.getLong("cnt"));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, SIZE / 3 - 1, SIZE / 3, SIZE / 3 + 1 })
    void having_limit(int limit) throws Exception {
        having_limit(limit, false);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, SIZE / 3 - 1, SIZE / 3, SIZE / 3 + 1 })
    void having_orderBy_limit(int limit) throws Exception {
        having_limit(limit, true);
    }

    private void having_limit(int limit, boolean order) throws Exception {
        var sql = "select zzz, count(*) cnt from " + TEST //
                + " group by zzz" //
                + " having count(*) > 1" //
                + (order ? " order by zzz" : "") //
                + " limit " + limit;
        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql);

        var expectedAllMap0 = TEST_LIST.stream().collect(Collectors.groupingBy(TestEntity::getZzz, Collectors.counting()));
        var expectedAllMap = expectedAllMap0.entrySet().stream() //
                .filter(entry -> entry.getValue() > 1) //
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
        if (order) {
            var expectedList = expectedAllMap.entrySet().stream() //
                    .sorted(Comparator.comparing(entry -> entry.getKey())) //
                    .limit(limit) //
                    .collect(Collectors.toList());
            assertEquals(expectedList.size(), list.size());
            int i = 0;
            for (var actual : list) {
                var expected = expectedList.get(i++);
                assertEquals(expected.getKey(), actual.getString("zzz"));
                assertEquals(expected.getValue(), actual.getLong("cnt"));
            }
        } else {
            int expectedSize = Math.min(limit, expectedAllMap.size());
            assertEquals(expectedSize, list.size());
            for (var actual : list) {
                String zzz = actual.getString("zzz");
                assertEquals(expectedAllMap.get(zzz), actual.getLong("cnt"));
            }
        }
    }
}
