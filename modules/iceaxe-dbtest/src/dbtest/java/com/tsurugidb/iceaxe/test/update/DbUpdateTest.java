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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * update test
 */
class DbUpdateTest extends DbTestTableTester {

    private static final int SIZE = 10;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(info);
    }

    @Test
    void updateAll() throws Exception {
        var sql = "update " + TEST //
                + " set" //
                + "  bar = 0," //
                + "  zzz = 'aaa'";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            int count = tm.executeAndGetCount(ps);
            assertUpdateCount(SIZE, count);
        }

        var list = selectAllFromTest();
        assertEquals(SIZE, list.size());
        for (var entity : list) {
            assertEquals(0L, entity.getBar());
            assertEquals("aaa", entity.getZzz());
        }
    }

    @Test
    void updateAllNull() throws Exception {
        var sql = "update " + TEST //
                + " set" //
                + "  bar = null," //
                + "  zzz = null";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            int count = tm.executeAndGetCount(ps);
            assertUpdateCount(SIZE, count);
        }

        var list = selectAllFromTest();
        assertEquals(SIZE, list.size());
        for (var entity : list) {
            assertNull(entity.getBar());
            assertNull(entity.getZzz());
        }
    }

    @Test
    void updateWhere() throws Exception {
        var sql = "update " + TEST //
                + " set" //
                + "  bar = 0," //
                + "  zzz = 'aaa'" //
                + " where foo % 2 = 0";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            int count = tm.executeAndGetCount(ps);
            assertUpdateCount(SIZE / 2, count);
        }

        var list = selectAllFromTest();
        assertEquals(SIZE, list.size());
        int i = 0;
        for (var entity : list) {
            if (entity.getFoo() % 2 == 0) {
                assertEquals(0L, entity.getBar());
                assertEquals("aaa", entity.getZzz());
            } else {
                assertEquals(i, entity.getBar());
                assertEquals(Integer.toString(i), entity.getZzz());
            }
            i++;
        }
    }

    @Test
    void updateNothing() throws Exception {
        var sql = "update " + TEST //
                + " set" //
                + "  bar = 0," //
                + "  zzz = 'aaa'" //
                + " where foo < 0";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            int count = tm.executeAndGetCount(ps);
            assertUpdateCount(0, count);
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void updateEntity() throws Exception {
        var sql = "update " + TEST //
                + " set" //
                + "  bar = :bar," //
                + "  zzz = :zzz" //
                + " where foo = :foo";
        var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                .addInt("foo", TestEntity::getFoo) //
                .addLong("bar", TestEntity::getBar) //
                .addString("zzz", TestEntity::getZzz);

        var updateEntity = new TestEntity(5, 55, "go");

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql, parameterMapping)) {
            int count = tm.executeAndGetCount(ps, updateEntity);
            assertUpdateCount(1, count);
        }

        var list = selectAllFromTest();
        assertEquals(SIZE, list.size());
        int i = 0;
        for (var entity : list) {
            if (entity.getFoo().equals(updateEntity.getFoo())) {
                assertEquals(updateEntity, entity);
            } else {
                var expected = createTestEntity(i);
                assertEquals(expected, entity);
            }
            i++;
        }
    }

    @Test
    void updateExpression() throws Exception {
        var sql = "update " + TEST //
                + " set" //
                + "  bar = bar + 1";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            int count = tm.executeAndGetCount(ps);
            assertUpdateCount(SIZE, count);
        }

        var list = selectAllFromTest();
        assertEquals(SIZE, list.size());
        int i = 0;
        for (var entity : list) {
            var expected = new TestEntity(i, i + 1, Integer.toString(i));
            assertEquals(expected, entity);
            i++;
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 0, 1, SIZE - 1, SIZE, SIZE + 1, -(SIZE - 1), -SIZE, -(SIZE + 1) })
    void updatePK_occ(int add) throws Exception {
        updatePK(add, TgTxOption.ofOCC());
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 0, 1, SIZE - 1, SIZE, SIZE + 1, -(SIZE - 1), -SIZE, -(SIZE + 1) })
    void updatePK_ltx(int add) throws Exception {
        updatePK(add, TgTxOption.ofLTX(TEST));
    }

    private void updatePK(int add, TgTxOption txOption) throws IOException, InterruptedException {
        String addText = (add >= 0) ? " + " + add : "" + add;
        var sql = "update " + TEST //
                + " set" //
                + "  foo = foo" + addText; // primary key

        var session = getSession();
        var tm = session.createTransactionManager(txOption);
        try (var ps = session.createStatement(sql)) {
            if (1 <= add && add <= SIZE - 1) {
                var e = assertThrowsExactly(TsurugiTmIOException.class, () -> tm.executeAndGetCount(ps));
                assertEqualsCode(SqlServiceCode.UNIQUE_CONSTRAINT_VIOLATION_EXCEPTION, e);
                assertEqualsTestTable(SIZE);
                return;
            } else {
                int count = tm.executeAndGetCount(ps);
                assertUpdateCount(SIZE, count);
            }
        }

        var expectedList = IntStream.range(0, SIZE).mapToObj(i -> createTestEntity(i)).peek(e -> e.setFoo(e.getFoo() + add)).collect(Collectors.toList());
        Collections.sort(expectedList, Comparator.comparing(TestEntity::getFoo));
        assertEqualsTestTable(expectedList);
    }

    @Test
    void updatePKNoChange() throws Exception {
        var sql = "update " + TEST //
                + " set" //
                + "  foo = foo"; // primary key

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            int count = tm.executeAndGetCount(ps);
            assertUpdateCount(SIZE, count);
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void insertUpdate() throws Exception {
        var insertEntity = new TestEntity(123, 456, "abc");
        var sql = "update " + TEST //
                + " set" //
                + "  bar = 789" //
                + " where foo = " + insertEntity.getFoo();

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.execute(tranasction -> {
            try (var insertPs = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                    var updatePs = session.createStatement(sql)) {
                // insert
                int insertCount = tranasction.executeAndGetCount(insertPs, insertEntity);
                assertUpdateCount(1, insertCount);
                // update
                int updateCount = tranasction.executeAndGetCount(updatePs);
                assertUpdateCount(1, updateCount);
            }

            // select
            try (var ps = session.createQuery(SELECT_SQL, SELECT_MAPPING)) {
                var list = tranasction.executeAndGetList(ps);
                assertInsertUpdate(insertEntity, 1, list);
            }
        });

        var list = selectAllFromTest();
        assertInsertUpdate(insertEntity, 1, list);
    }

    @Test
    void insertUpdateNoCheck() throws Exception {
        var insertEntity = new TestEntity(123, 456, "abc");
        var sql = "update " + TEST //
                + " set" //
                + "  bar = 789" //
                + " where foo = " + insertEntity.getFoo();

        var updateCount = new AtomicInteger();
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.execute(tranasction -> {
            try (var insertPs = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                    var updatePs = session.createStatement(sql)) {
                // executeの結果を確認せずに次のSQLを実行すると、同一トランザクション内でもSQLの実行順序が保証されない
                // insert
                var insertResult = insertPs.execute(tranasction, insertEntity);
                // update
                var updateResult = updatePs.execute(tranasction);

                updateCount.set(updateResult.getUpdateCount());
                if (updateCount.get() != 1) {
                    LOG.info("insertUpdateNoCheck().updateCount={}", updateCount);
                    assertEquals(0, updateCount.get());
                }
                assertEquals(1, insertResult.getUpdateCount());
            }

            // select
            try (var ps = session.createQuery(SELECT_SQL, SELECT_MAPPING)) {
                var list = tranasction.executeAndGetList(ps);
                assertInsertUpdate(insertEntity, updateCount.get(), list);
            }
        });

        var list = selectAllFromTest();
        assertInsertUpdate(insertEntity, updateCount.get(), list);
    }

    private static void assertInsertUpdate(TestEntity insertEntity, int updateCount, List<TestEntity> list) {
        assertEquals(SIZE + 1, list.size());
        for (var entity : list) {
            if (entity.getFoo().equals(insertEntity.getFoo())) {
                if (updateCount == 1) {
                    assertEquals(789L, entity.getBar());
                } else {
                    assertEquals(insertEntity.getBar(), entity.getBar());
                }
            } else {
                assertEquals((long) entity.getFoo(), entity.getBar());
            }
        }
    }

    @Test
    void updateUpdate() throws Exception {
        int foo = 1;
        var bar = TgBindVariable.ofLong("bar");
        var sql = "update " + TEST //
                + " set" //
                + "  bar = " + bar //
                + " where foo = " + foo;
        var parameterMapping = TgParameterMapping.of(bar);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.execute(tranasction -> {
            // update
            try (var ps = session.createStatement(sql, parameterMapping)) {
                {
                    var parameter = TgBindParameters.of(bar.bind(101));
                    int count = tranasction.executeAndGetCount(ps, parameter);
                    assertUpdateCount(1, count);
                }
                {
                    var parameter = TgBindParameters.of(bar.bind(102));
                    int count = tranasction.executeAndGetCount(ps, parameter);
                    assertUpdateCount(1, count);
                }
            }

            // select
            try (var ps = session.createQuery(SELECT_SQL, SELECT_MAPPING)) {
                var list = tranasction.executeAndGetList(ps);
                assertEquals(SIZE, list.size());
                for (var entity : list) {
                    if (entity.getFoo().equals(foo)) {
                        assertEquals(102L, entity.getBar());
                    } else {
                        assertEquals((long) entity.getFoo(), entity.getBar());
                    }
                }
            }
        });

        var list = selectAllFromTest();
        assertEquals(SIZE, list.size());
        for (var entity : list) {
            if (entity.getFoo().equals(foo)) {
                assertEquals(102L, entity.getBar());
            } else {
                assertEquals((long) entity.getFoo(), entity.getBar());
            }
        }
    }
}
