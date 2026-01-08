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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * select where expression test
 */
class DbSelectWhereExpressionTest extends DbTestTableTester {

    private static final int SIZE = 200;
    private static TestEntity NULL_ENTITY = new TestEntity(999, null, null);

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectWhereExpressionTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);
        insertTestTable(NULL_ENTITY);

        logInitEnd(LOG, info);
    }

    @Test
    void invalidExpression() throws Exception {
        var sql = SELECT_SQL + " where foo === 1";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                tm.executeAndGetList(ps);
            });
            assertEqualsCode(SqlServiceCode.SYNTAX_EXCEPTION, e);
            assertContains("appeared unexpected token: \"=\"", e.getMessage());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "=", "<>", "<", ">", "<=", ">=", "!=" })
    void eqNull(String expression) throws Exception {
        var sql = SELECT_SQL + " where bar " + expression + " null";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var list = tm.executeAndGetList(ps);
            assertEquals(0, list.size());
        }
    }

    @Test
    void isNull() throws Exception {
        var tm = createTransactionManagerOcc(getSession());

        var sql = SELECT_SQL + " where bar is null";
        var list = tm.executeAndGetList(sql, SELECT_MAPPING);
        assertEquals(1, list.size());
        for (var entity : list) {
            assertNull(entity.getBar());
        }
    }

    @Test
    void isNotNull() throws Exception {
        var tm = createTransactionManagerOcc(getSession());

        var sql = SELECT_SQL + " where bar is not null";
        var list = tm.executeAndGetList(sql, SELECT_MAPPING);
        assertEquals(SIZE, list.size());
        for (var entity : list) {
            assertNotNull(entity.getBar());
        }
    }

    @Test
    void isTrue() throws Exception {
        var tm = createTransactionManagerOcc(getSession());

        {
            var sql = SELECT_SQL + " where (1=1) is true";
            var list = tm.executeAndGetList(sql);
            assertEquals(SIZE + 1, list.size());
        }
        {
            var sql = SELECT_SQL + " where (foo<10) is true";
            var list = tm.executeAndGetList(sql, SELECT_MAPPING);
            assertEquals(10, list.size());
            for (var entity : list) {
                assertTrue(entity.getFoo() < 10);
            }
        }
        {
            var sql = SELECT_SQL + " where (bar<10) is true";
            var list = tm.executeAndGetList(sql, SELECT_MAPPING);
            assertEquals(10, list.size());
            for (var entity : list) {
                assertTrue(entity.getBar() < 10);
            }
        }
    }

    @Test
    void isNotTrue() throws Exception {
        var tm = createTransactionManagerOcc(getSession());

        {
            var sql = SELECT_SQL + " where (1=1) is not true";
            var list = tm.executeAndGetList(sql);
            assertEquals(0, list.size());
        }
        {
            var sql = SELECT_SQL + " where (foo<10) is not true";
            var list = tm.executeAndGetList(sql, SELECT_MAPPING);
            assertEquals((SIZE + 1) - 10, list.size());
            for (var entity : list) {
                assertTrue(entity.getFoo() >= 10);
            }
        }
        {
            var sql = SELECT_SQL + " where (bar<10) is not true";
            var list = tm.executeAndGetList(sql, SELECT_MAPPING);
            assertEquals((SIZE + 1) - 10, list.size());
            for (var entity : list) {
                if (entity.getBar() == null) {
                    // OK
                } else {
                    assertTrue(entity.getBar() >= 10);
                }
            }
        }
    }

    @Test
    void isFalse() throws Exception {
        var tm = createTransactionManagerOcc(getSession());

        {
            var sql = SELECT_SQL + " where (1=0) is false";
            var list = tm.executeAndGetList(sql);
            assertEquals(SIZE + 1, list.size());
        }
        {
            var sql = SELECT_SQL + " where (foo<10) is false";
            var list = tm.executeAndGetList(sql, SELECT_MAPPING);
            assertEquals((SIZE + 1) - 10, list.size());
            for (var entity : list) {
                assertTrue(entity.getFoo() >= 10);
            }
        }
        {
            var sql = SELECT_SQL + " where (bar<10) is false";
            var list = tm.executeAndGetList(sql, SELECT_MAPPING);
            assertEquals(SIZE - 10, list.size());
            for (var entity : list) {
                assertTrue(entity.getBar() >= 10);
            }
        }
    }

    @Test
    void isNotFalse() throws Exception {
        var tm = createTransactionManagerOcc(getSession());

        {
            var sql = SELECT_SQL + " where (1=0) is not false";
            var list = tm.executeAndGetList(sql);
            assertEquals(0, list.size());
        }
        {
            var sql = SELECT_SQL + " where (foo<10) is not false";
            var list = tm.executeAndGetList(sql, SELECT_MAPPING);
            assertEquals(10, list.size());
            for (var entity : list) {
                assertTrue(entity.getFoo() < 10);
            }
        }
        {
            var sql = SELECT_SQL + " where (bar<10) is not false";
            var list = tm.executeAndGetList(sql, SELECT_MAPPING);
            assertEquals(10 + 1, list.size());
            for (var entity : list) {
                if (entity.getBar() == null) {
                    // OK
                } else {
                    assertTrue(entity.getBar() < 10);
                }
            }
        }
    }

    @Test
    void isUnknown() throws Exception {
        var tm = createTransactionManagerOcc(getSession());

        {
            var sql = SELECT_SQL + " where (1=0) is unknown";
            var list = tm.executeAndGetList(sql);
            assertEquals(0, list.size());
        }
        {
            var sql = SELECT_SQL + " where null is unknown";
            var list = tm.executeAndGetList(sql);
            assertEquals(SIZE + 1, list.size());
        }
        {
            var sql = SELECT_SQL + " where (1=null) is unknown";
            var list = tm.executeAndGetList(sql);
            assertEquals(SIZE + 1, list.size());
        }
    }

    @Test
    void isNotUnknown() throws Exception {
        var tm = createTransactionManagerOcc(getSession());

        {
            var sql = SELECT_SQL + " where (1=0) is not unknown";
            var list = tm.executeAndGetList(sql);
            assertEquals(SIZE + 1, list.size());
        }
        {
            var sql = SELECT_SQL + " where null is not unknown";
            var list = tm.executeAndGetList(sql);
            assertEquals(0, list.size());
        }
        {
            var sql = SELECT_SQL + " where (1=null) is not unknown";
            var list = tm.executeAndGetList(sql);
            assertEquals(0, list.size());
        }
    }

    @Test
    void in() throws Exception {
        var expectedList = List.of(2L, 4L, 5L, 7L);
        var sql = SELECT_SQL + " where bar in (" + expectedList.stream().map(n -> Long.toString(n)).collect(Collectors.joining(",")) + ")";
//      var sql = SELECT_SQL + " where " + expectedList.stream().map(n -> "bar=" + n).collect(Collectors.joining(" or "));

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            var list = tm.executeAndGetList(ps);
            assertWhere(entity -> {
                if (entity.getBar() == null) {
                    return false;
                }
                return expectedList.contains(entity.getBar());
            }, list);
        }
    }

    @Test
    void notIn() throws Exception {
        var expectedList = List.of(2L, 4L, 5L, 7L);
        var sql = SELECT_SQL + " where bar not in (" + expectedList.stream().map(n -> Long.toString(n)).collect(Collectors.joining(",")) + ")";
//      var sql = SELECT_SQL + " where not(" + expectedList.stream().map(n -> "bar=" + n).collect(Collectors.joining(" or "))+ ")";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            var list = tm.executeAndGetList(ps);
            assertWhere(entity -> {
                if (entity.getBar() == null) {
                    return false;
                }
                return !expectedList.contains(entity.getBar());
            }, list);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "symmetric", "asymmetric" })
    void between(String betweenType) throws Exception {
        int start = 4;
        int end = 6;
        var sql = SELECT_SQL + " where bar between " + betweenType + " " + start + " and " + end;
//      var sql = SELECT_SQL + " where " + start + " <= bar and bar <= " + end;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            var list = tm.executeAndGetList(ps);
            assertWhere(entity -> {
                if (entity.getBar() == null) {
                    return false;
                }
                return start <= entity.getBar() && entity.getBar() <= end;
            }, list);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "symmetric", "asymmetric" })
    void betweenSymmetric(String betweenType) throws Exception {
        int start = 6;
        int end = 4;
        var sql = SELECT_SQL + " where bar between " + betweenType + " " + start + " and " + end;
//      var sql = SELECT_SQL + " where " + end + " <= bar and bar <= " + start;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            var list = tm.executeAndGetList(ps);
            if (betweenType.equals("symmetric")) {
                assertWhere(entity -> {
                    if (entity.getBar() == null) {
                        return false;
                    }
                    return end <= entity.getBar() && entity.getBar() <= start;
                }, list);
            } else {
                assertEquals(0, list.size());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "symmetric", "asymmetric" })
    void notBetween(String betweenType) throws Exception {
        int start = 4;
        int end = 6;
        var sql = SELECT_SQL + " where bar not between " + betweenType + " " + start + " and " + end;
//      var sql = SELECT_SQL + " where not(" + start + " <= bar and bar <= " + end + ")";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            var list = tm.executeAndGetList(ps);
            assertWhere(entity -> {
                if (entity.getBar() == null) {
                    return false;
                }
                return !(start <= entity.getBar() && entity.getBar() <= end);
            }, list);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "symmetric", "asymmetric" })
    void notBetweenSymmetric(String betweenType) throws Exception {
        int start = 6;
        int end = 4;
        var sql = SELECT_SQL + " where bar not between " + betweenType + " " + start + " and " + end;
//      var sql = SELECT_SQL + " where not(" + end + " <= bar and bar <= " + start + ")";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            var list = tm.executeAndGetList(ps);
            if (betweenType.equals("symmetric")) {
                assertWhere(entity -> {
                    if (entity.getBar() == null) {
                        return false;
                    }
                    return !(end <= entity.getBar() && entity.getBar() <= start);
                }, list);
            } else {
                assertWhere(entity -> entity.getBar() != null, list);
            }
        }
    }

    @Test
    void like() throws Exception {
        testLike("like '1%'", zzz -> zzz.startsWith("1"));
    }

    @Test
    void like_ends() throws Exception {
        testLike("like '%1'", zzz -> zzz.endsWith("1"));
    }

    @Test
    void like_contains() throws Exception {
        testLike("like '%1%'", zzz -> zzz.contains("1"));
    }

    @Test
    void like2() throws Exception {
        testLike("like '1_'", zzz -> zzz.length() == 2 && zzz.startsWith("1"));
    }

    @Test
    void like2_ends() throws Exception {
        testLike("like '_1'", zzz -> zzz.length() == 2 && zzz.endsWith("1"));
    }

    @Test
    void notLike() throws Exception {
        testLike("not like '1%'", zzz -> !zzz.startsWith("1"));
    }

    private void testLike(String like, Predicate<String> predicate) throws IOException, InterruptedException {
        var sql = SELECT_SQL + " where zzz " + like + " order by foo";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            var list = tm.executeAndGetList(ps);
            assertWhere(entity -> {
                String zzz = entity.getZzz();
                if (zzz == null) {
                    return false;
                }
                return predicate.test(zzz);
            }, list);
        }
    }

    private static void assertWhere(Predicate<TestEntity> where, List<TestEntity> actualList) {
        var expectedMap = expectedMap(where);
        assertEquals(expectedMap.size(), actualList.size());
        for (var actual : actualList) {
            assertTrue(expectedMap.containsKey(actual.getFoo()));
        }
    }

    private static Map<Integer, TestEntity> expectedMap(Predicate<TestEntity> where) {
        var map = new HashMap<Integer, TestEntity>();
        for (int i = 0; i < SIZE; i++) {
            var entity = createTestEntity(i);
            if (where.test(entity)) {
                map.put(entity.getFoo(), entity);
            }
        }

        var entity = NULL_ENTITY;
        if (where.test(entity)) {
            map.put(entity.getFoo(), entity);
        }

        return map;
    }
}
