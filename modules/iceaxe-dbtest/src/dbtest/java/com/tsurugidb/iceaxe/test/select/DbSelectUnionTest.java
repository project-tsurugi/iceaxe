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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;

/**
 * select union test
 */
class DbSelectUnionTest extends DbTestTableTester {

    private static final String TEST2 = "test2";
    private static final List<TestEntity> TEST_LIST;
    private static final List<TestEntity> TEST2_LIST;
    static {
        var list = new ArrayList<TestEntity>();
        for (int i = 0; i < 6; i++) {
            int key = i;
            var entity = new TestEntity(key, key, TEST);
            list.add(entity);
            if (key == 1 || key == 3 || key == 4) {
                list.add(entity);
            }
        }
        list.add(new TestEntity(2, 2, "COMMON"));
        list.add(new TestEntity(4, null, TEST));
        list.add(new TestEntity(4, null, TEST));
        list.add(new TestEntity(9, null, "COMMON"));
        list.add(new TestEntity(9, null, "COMMON"));
        TEST_LIST = list;
    }
    static {
        var list = new ArrayList<TestEntity>();
        for (int i = 0; i < 5; i++) {
            int key = i + 2;
            var entity = new TestEntity(key, key, TEST2);
            list.add(entity);
            if (key == 4 || key == 6) {
                list.add(entity);
            }
        }
        list.add(new TestEntity(2, 2, "COMMON"));
        list.add(new TestEntity(4, null, TEST2));
        list.add(new TestEntity(4, null, TEST2));
        list.add(new TestEntity(9, null, "COMMON"));
        list.add(new TestEntity(9, null, "COMMON"));
        TEST2_LIST = list;
    }

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectUnionTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable(false);
        insertTable(TEST, TEST_LIST);
        dropTable(TEST2);
        createTest2Table();
        insertTable(TEST2, TEST2_LIST);

        logInitEnd(LOG, info);
    }

    private static void createTest2Table() throws IOException, InterruptedException {
        String sql = CREATE_TEST_NO_PK_SQL.replace(TEST, TEST2);
        executeDdl(getSession(), sql, TEST2);
    }

    private static void insertTable(String tableName, List<TestEntity> list) throws IOException, InterruptedException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session, "insert" + tableName, 3);
        try (var ps = session.createStatement(INSERT_SQL.replace(TEST, tableName), INSERT_MAPPING)) {
            tm.execute(transaction -> {
                for (var entity : list) {
                    transaction.executeAndGetCount(ps, entity);
                }
                return;
            });
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "union", "union all", "union distinct" })
    void union(String union) throws Exception {
        var sql = "select * from " + TEST + "\n"//
                + union + "\n" //
                + "select * from " + TEST2 + "\n" //
                + "order by zzz, foo, bar";
        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql);
        sortWorkAroundZzzFoo(list);

        var expected = new ArrayList<TestEntity>(TEST_LIST.size() + TEST2_LIST.size());
        expected.addAll(TEST_LIST);
        expected.addAll(TEST2_LIST);
        if (!union.contains("all")) {
            distinctAllColumn(expected);
        }
        sortZzzFoo(expected);
        assertAllColumn(expected, list);
    }

    @ParameterizedTest
    @ValueSource(strings = { "union", "union all", "union distinct" })
    void union_limit(String union) throws Exception {
        var expected = new ArrayList<TestEntity>(TEST_LIST.size() + TEST2_LIST.size());
        expected.addAll(TEST_LIST);
        expected.addAll(TEST2_LIST);
        if (!union.contains("all")) {
            distinctAllColumn(expected);
        }
        sortZzzFoo(expected);

        int limit = expected.size() - 5;
        while (expected.size() > limit) {
            expected.remove(expected.size() - 1);
        }

        // TODO union + limit
//        var sql = "select * from " + TEST + "\n"//
//                + union + "\n" //
//                + "select * from " + TEST2 + "\n" //
//                + "order by zzz, foo, bar\n" //
//                + "limit " + limit;
        var sql = "select * from (\n" //
                + "select * from " + TEST + "\n"//
                + union + "\n" //
                + "select * from " + TEST2 + "\n" //
                + ") t\n" //
                + "order by zzz, foo, bar\n" //
                + "limit " + limit;
        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql);
        sortWorkAroundZzzFoo(list);

        assertAllColumn(expected, list);
    }

    @ParameterizedTest
    @ValueSource(strings = { "union", "union all", "union distinct" })
    @Disabled // TODO remove Disabled. union + limit
    void union_limit2(String union) throws Exception {
        int limit;
        {
            var expected = new ArrayList<TestEntity>(TEST_LIST.size() + TEST2_LIST.size());
            expected.addAll(TEST_LIST);
            expected.addAll(TEST2_LIST);
            if (!union.contains("all")) {
                distinctAllColumn(expected);
            }
            limit = expected.size() - 5;
        }

        var sql = "select * from " + TEST + "\n"//
                + union + "\n" //
                + "select * from " + TEST2;
        var sql1 = sql + "\n" //
                + "order by zzz, foo, bar\n" //
                + "limit " + limit;
        var sql2 = "select * from (\n" + sql + ") t\n" //
                + "order by zzz, foo, bar\n" //
                + "limit " + limit;

        var tm = createTransactionManagerOcc(getSession());
        var list1 = tm.executeAndGetList(sql1, SELECT_MAPPING);
        var list2 = tm.executeAndGetList(sql2, SELECT_MAPPING);
        assertEquals(list2, list1);
    }

    @ParameterizedTest
    @ValueSource(strings = { "except", "except distinct" }) // TODO "except all"
    void except(String except) throws Exception {
        var sql = "select * from " + TEST + "\n"//
                + except + "\n" //
                + "select * from " + TEST2 + "\n" //
                + "order by zzz, foo, bar";
        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql);
        sortWorkAroundZzzFoo(list);

        var expected = new ArrayList<TestEntity>(TEST_LIST.size());
        expected.addAll(TEST_LIST);
        for (var entity : TEST2_LIST) {
            expected.remove(entity);
        }
        if (!except.contains("all")) {
            distinctAllColumn(expected);
        }
        sortZzzFoo(expected);
        assertAllColumn(expected, list);
    }

    @ParameterizedTest
    @ValueSource(strings = { "intersect", "intersect distinct" }) // TODO "intersect all"
    void intersect(String intersect) throws Exception {
        var sql = "select * from " + TEST + "\n"//
                + intersect + "\n" //
                + "select * from " + TEST2 + "\n" //
                + "order by zzz, foo, bar";
        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql);
        sortWorkAroundZzzFoo(list);

        var expected = new ArrayList<TestEntity>(TEST2_LIST.size());
        var set = Set.copyOf(TEST_LIST);
        for (var entity : TEST2_LIST) {
            if (set.contains(entity)) {
                expected.add(entity);
            }
        }
        if (!intersect.contains("all")) {
            distinctAllColumn(expected);
        }
        sortZzzFoo(expected);
        assertAllColumn(expected, list);
    }

    private static void distinctAllColumn(List<TestEntity> list) {
        var set = new HashSet<TestEntity>();
        for (var i = list.iterator(); i.hasNext();) {
            var entity = i.next();
            if (set.contains(entity)) {
                i.remove();
            } else {
                set.add(entity);
            }
        }
    }

    private static void sortWorkAroundZzzFoo(List<TsurugiResultEntity> list) { // TODO remove sort (for union+order by)
        var c = Comparator.<TsurugiResultEntity, String>comparing(entity -> entity.getString("zzz")) //
                .thenComparing(entity -> entity.getInt("foo")) //
                .thenComparing(entity -> entity.getLongOrNull("bar"), Comparator.nullsFirst(Comparator.naturalOrder()));
        list.sort(c);
    }

    private static void sortZzzFoo(List<TestEntity> list) {
        var c = Comparator.comparing(TestEntity::getZzz) //
                .thenComparing(TestEntity::getFoo) //
                .thenComparing(TestEntity::getBar, Comparator.nullsFirst(Comparator.naturalOrder()));
        list.sort(c);
    }

    private void assertAllColumn(List<TestEntity> expectedList, List<TsurugiResultEntity> actualList) {
        assertEquals(expectedList.size(), actualList.size());
        int i = 0;
        for (var actual : actualList) {
            var expected = expectedList.get(i++);
            assertEquals(expected.getFoo(), actual.getIntOrNull("foo"));
            assertEquals(expected.getBar(), actual.getLongOrNull("bar"));
            assertEquals(expected.getZzz(), actual.getString("zzz"));
        }
    }
}
