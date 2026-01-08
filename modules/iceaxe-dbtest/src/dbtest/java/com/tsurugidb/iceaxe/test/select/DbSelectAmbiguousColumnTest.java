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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.result.IceaxeResultNameList.IceaxeAmbiguousNamePolicy;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;

/**
 * select ambiguous column test
 */
class DbSelectAmbiguousColumnTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectAmbiguousColumnTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(LOG, info);
    }

    private static class TestCrossJoinEntity {
        private int foo1;
        private long bar1;
        private String zzz1;
        private int foo2;
        private long bar2;
        private String zzz2;

        public void setFoo1(int foo1) {
            this.foo1 = foo1;
        }

        public void setBar1(long bar1) {
            this.bar1 = bar1;
        }

        public void setZzz1(String zzz1) {
            this.zzz1 = zzz1;
        }

        public void setFoo2(int foo2) {
            this.foo2 = foo2;
        }

        public void setBar2(long bar2) {
            this.bar2 = bar2;
        }

        public void setZzz2(String zzz2) {
            this.zzz2 = zzz2;
        }

        public void setEntity1(TestEntity entity) {
            this.foo1 = entity.getFoo();
            this.bar1 = entity.getBar();
            this.zzz1 = entity.getZzz();
        }

        public void setEntity2(TestEntity entity) {
            this.foo2 = entity.getFoo();
            this.bar2 = entity.getBar();
            this.zzz2 = entity.getZzz();
        }

        @Override
        public int hashCode() {
            return Objects.hash(bar1, bar2, foo1, foo2, zzz1, zzz2);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            TestCrossJoinEntity other = (TestCrossJoinEntity) obj;
            return bar1 == other.bar1 && bar2 == other.bar2 && foo1 == other.foo1 && foo2 == other.foo2 && Objects.equals(zzz1, other.zzz1) && Objects.equals(zzz2, other.zzz2);
        }

        @Override
        public String toString() {
            return "TestCrossJoinEntity [foo1=" + foo1 + ", bar1=" + bar1 + ", zzz1=" + zzz1 + ", foo2=" + foo2 + ", bar2=" + bar2 + ", zzz2=" + zzz2 + "]";
        }
    }

    private static final List<TestCrossJoinEntity> EXPECTED_LIST;
    static {
        var list = new ArrayList<TestCrossJoinEntity>(SIZE * SIZE);
        for (int i = 0; i < SIZE; i++) {
            var srcEntity1 = createTestEntity(i);
            for (int j = 0; j < SIZE; j++) {
                var entity = new TestCrossJoinEntity();
                entity.setEntity1(srcEntity1);
                entity.setEntity2(createTestEntity(j));
                list.add(entity);
            }
        }
        EXPECTED_LIST = list;
    }

    @Test
    void record_currentColumn() throws Exception {
        var sql = "select * from " + TEST + ", " + TEST;
        var resultMapping = TgResultMapping.of(record -> {
            var entity = new TestCrossJoinEntity();
            for (int i = 0; record.moveCurrentColumnNext(); i++) {
                String name = record.getCurrentColumnName();
                var value = record.fetchCurrentColumnValue();
                switch (i) {
                case 0:
                    assertEquals("foo", name);
                    entity.setFoo1((Integer) value);
                    break;
                case 1:
                    assertEquals("bar", name);
                    entity.setBar1((Long) value);
                    break;
                case 2:
                    assertEquals("zzz", name);
                    entity.setZzz1((String) value);
                    break;
                case 3:
                    assertEquals("foo", name);
                    entity.setFoo2((Integer) value);
                    break;
                case 4:
                    assertEquals("bar", name);
                    entity.setBar2((Long) value);
                    break;
                case 5:
                    assertEquals("zzz", name);
                    entity.setZzz2((String) value);
                    break;
                default:
                    throw new AssertionError(i);
                }
            }
            return entity;
        });

        var tm = createTransactionManagerOcc(getSession());
        var actualList = tm.executeAndGetList(sql, resultMapping);

        assertResult(actualList);
    }

    @Test
    void record_getByIndex() throws Exception {
        var sql = "select * from " + TEST + ", " + TEST;
        var resultMapping = TgResultMapping.of(record -> {
            var entity = new TestCrossJoinEntity();
            int i = 0;
            entity.setFoo1(record.getInt(i++));
            entity.setBar1(record.getLong(i++));
            entity.setZzz1(record.getString(i++));
            entity.setFoo2(record.getInt(i++));
            entity.setBar2(record.getLong(i++));
            entity.setZzz2(record.getString(i++));
            return entity;
        });

        var tm = createTransactionManagerOcc(getSession());
        var actualList = tm.executeAndGetList(sql, resultMapping);

        assertResult(actualList);
    }

    @Test
    void record_getByName() throws Exception {
        var sql = "select * from " + TEST + ", " + TEST;
        var resultMapping = TgResultMapping.of(record -> {
            var entity = new TestCrossJoinEntity();
            record.setAmbiguousNamePolicy(IceaxeAmbiguousNamePolicy.FIRST);
            entity.setFoo1(record.getInt("foo"));
            entity.setBar1(record.getLong("bar"));
            entity.setZzz1(record.getString("zzz"));
            record.setAmbiguousNamePolicy(IceaxeAmbiguousNamePolicy.LAST);
            entity.setFoo2(record.getInt("foo"));
            entity.setBar2(record.getLong("bar"));
            entity.setZzz2(record.getString("zzz"));
            return entity;
        });

        var tm = createTransactionManagerOcc(getSession());
        var actualList = tm.executeAndGetList(sql, resultMapping);

        assertResult(actualList);
    }

    @Test
    void record_getByIndexFromName() throws Exception {
        var sql = "select * from " + TEST + ", " + TEST;
        var resultMapping = TgResultMapping.of(record -> {
            var entity = new TestCrossJoinEntity();
            entity.setFoo1(record.getInt(record.getIndex("foo", 0)));
            entity.setBar1(record.getLong(record.getIndex("bar", 0)));
            entity.setZzz1(record.getString(record.getIndex("zzz", 0)));
            entity.setFoo2(record.getInt(record.getIndex("foo", 1)));
            entity.setBar2(record.getLong(record.getIndex("bar", 1)));
            entity.setZzz2(record.getString(record.getIndex("zzz", 1)));
            return entity;
        });

        var tm = createTransactionManagerOcc(getSession());
        var actualList = tm.executeAndGetList(sql, resultMapping);

        assertResult(actualList);
    }

    @Test
    void record_next() throws Exception {
        var sql = "select * from " + TEST + ", " + TEST;
        var resultMapping = TgResultMapping.of(record -> {
            var entity = new TestCrossJoinEntity();
            entity.setFoo1(record.nextInt());
            entity.setBar1(record.nextLong());
            entity.setZzz1(record.nextString());
            entity.setFoo2(record.nextInt());
            entity.setBar2(record.nextLong());
            entity.setZzz2(record.nextString());
            return entity;
        });

        var tm = createTransactionManagerOcc(getSession());
        var actualList = tm.executeAndGetList(sql, resultMapping);

        assertResult(actualList);
    }

    @Test
    void entity_getByIndex() throws Exception {
        var sql = "select * from " + TEST + ", " + TEST;

        var tm = createTransactionManagerOcc(getSession());
        List<TsurugiResultEntity> resultList = tm.executeAndGetList(sql);

        var actualList = new ArrayList<TestCrossJoinEntity>(resultList.size());
        for (var result : resultList) {
            var entity = new TestCrossJoinEntity();
            int i = 0;
            entity.setFoo1(result.getInt(i++));
            entity.setBar1(result.getLong(i++));
            entity.setZzz1(result.getString(i++));
            entity.setFoo2(result.getInt(i++));
            entity.setBar2(result.getLong(i++));
            entity.setZzz2(result.getString(i++));
            actualList.add(entity);
        }
        assertResult(actualList);
    }

    @Test
    void entity_getByName() throws Exception {
        var sql = "select * from " + TEST + ", " + TEST;

        var tm = createTransactionManagerOcc(getSession());
        List<TsurugiResultEntity> resultList = tm.executeAndGetList(sql);

        var actualList = new ArrayList<TestCrossJoinEntity>(resultList.size());
        for (var result : resultList) {
            var entity = new TestCrossJoinEntity();
            result.setAmbiguousNamePolicy(IceaxeAmbiguousNamePolicy.FIRST);
            entity.setFoo1(result.getInt("foo"));
            entity.setBar1(result.getLong("bar"));
            entity.setZzz1(result.getString("zzz"));
            result.setAmbiguousNamePolicy(IceaxeAmbiguousNamePolicy.LAST);
            entity.setFoo2(result.getInt("foo"));
            entity.setBar2(result.getLong("bar"));
            entity.setZzz2(result.getString("zzz"));
            actualList.add(entity);
        }
        assertResult(actualList);
    }

    @Test
    void entity_getByIndexFromName() throws Exception {
        var sql = "select * from " + TEST + ", " + TEST;

        var tm = createTransactionManagerOcc(getSession());
        List<TsurugiResultEntity> resultList = tm.executeAndGetList(sql);

        var actualList = new ArrayList<TestCrossJoinEntity>(resultList.size());
        for (var result : resultList) {
            var entity = new TestCrossJoinEntity();
            entity.setFoo1(result.getInt(result.getIndex("foo", 0)));
            entity.setBar1(result.getLong(result.getIndex("bar", 0)));
            entity.setZzz1(result.getString(result.getIndex("zzz", 0)));
            entity.setFoo2(result.getInt(result.getIndex("foo", 1)));
            entity.setBar2(result.getLong(result.getIndex("bar", 1)));
            entity.setZzz2(result.getString(result.getIndex("zzz", 1)));
            actualList.add(entity);
        }
        assertResult(actualList);
    }

    @Test
    void mapping_sequential() throws Exception {
        var sql = "select * from " + TEST + ", " + TEST;
        var resultMapping = TgResultMapping.of(TestCrossJoinEntity::new) //
                .addInt(TestCrossJoinEntity::setFoo1) //
                .addLong(TestCrossJoinEntity::setBar1) //
                .addString(TestCrossJoinEntity::setZzz1) //
                .addInt(TestCrossJoinEntity::setFoo2) //
                .addLong(TestCrossJoinEntity::setBar2) //
                .addString(TestCrossJoinEntity::setZzz2);

        var tm = createTransactionManagerOcc(getSession());
        var actualList = tm.executeAndGetList(sql, resultMapping);

        assertResult(actualList);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void mapping_name(int pattern) throws Exception {
        var sql = "select * from " + TEST + ", " + TEST;
        TgResultMapping<TestCrossJoinEntity> resultMapping;
        switch (pattern) {
        case 0:
            resultMapping = TgResultMapping.of(TestCrossJoinEntity::new) //
                    .addInt("foo", TestCrossJoinEntity::setFoo1) //
                    .addLong("bar", TestCrossJoinEntity::setBar1) //
                    .addString("zzz", TestCrossJoinEntity::setZzz1) //
                    .addInt("foo", TestCrossJoinEntity::setFoo2) //
                    .addLong("bar", TestCrossJoinEntity::setBar2) //
                    .addString("zzz", TestCrossJoinEntity::setZzz2);
            break;
        case 1:
            resultMapping = TgResultMapping.of(TestCrossJoinEntity::new) //
                    .addInt("foo", TestCrossJoinEntity::setFoo1) //
                    .addInt("foo", TestCrossJoinEntity::setFoo2) //
                    .addLong("bar", TestCrossJoinEntity::setBar1) //
                    .addLong("bar", TestCrossJoinEntity::setBar2) //
                    .addString("zzz", TestCrossJoinEntity::setZzz1) //
                    .addString("zzz", TestCrossJoinEntity::setZzz2);
            break;
        default:
            throw new AssertionError(pattern);
        }

        var tm = createTransactionManagerOcc(getSession());
        var actualList = tm.executeAndGetList(sql, resultMapping);

        assertResult(actualList);
    }

    private static void assertResult(List<TestCrossJoinEntity> actualList) {
        var expectedSet = new HashSet<TestCrossJoinEntity>(EXPECTED_LIST);
        assertEquals(expectedSet.size(), actualList.size());
        for (var actual : actualList) {
            assertTrue(expectedSet.remove(actual));
        }
        assertTrue(expectedSet.isEmpty());
    }
}
