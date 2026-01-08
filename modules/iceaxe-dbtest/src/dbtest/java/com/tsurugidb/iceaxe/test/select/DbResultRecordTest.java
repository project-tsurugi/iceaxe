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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;

/**
 * {@link TsurugiResultRecord} test
 */
class DbResultRecordTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbResultRecordTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(LOG, info);
    }

    @Test
    void getInt_null() throws Exception {
        var sql = "select max(foo) as foo from " + TEST + " where foo < 0";
        var resultMapping = TgResultMapping.of(record -> record.getInt("foo"));

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, resultMapping)) {
            var e = assertThrowsExactly(NullPointerException.class, () -> {
                tm.execute(transaction -> {
                    transaction.executeAndFindRecord(ps);
                });
            });
            assertEqualsMessage("getInt(foo) is null", e);
        }
    }

    @Test
    void nextInt_null() throws Exception {
        var sql = "select max(foo) from " + TEST + " where foo < 0";
        var resultMapping = TgResultMapping.of(record -> record.nextInt());

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, resultMapping)) {
            var e = assertThrowsExactly(NullPointerException.class, () -> {
                tm.execute(transaction -> {
                    transaction.executeAndFindRecord(ps);
                });
            });
            assertEqualsMessage("nextInt(0) is null", e);
        }
    }

    @Test
    void fewColumn_currentColumn() throws Exception {
        var sql = "select * from " + TEST + " order by foo";
        var resultMapping = TgResultMapping.of(record -> {
            var entity = new TestEntity();
            assertTrue(record.moveCurrentColumnNext());
            entity.setFoo((Integer) record.fetchCurrentColumnValue());
            assertTrue(record.moveCurrentColumnNext());
            entity.setBar((Long) record.fetchCurrentColumnValue());
            return entity;
        });

        var tm = createTransactionManagerOcc(getSession());
        var actualList = tm.executeAndGetList(sql, resultMapping);

        assertFewColumn(actualList);
    }

    @Test
    void fewColumn_getByIndex() throws Exception {
        var sql = "select * from " + TEST + " order by foo";
        var resultMapping = TgResultMapping.of(record -> {
            var entity = new TestEntity();
            int i = 0;
            entity.setFoo(record.getInt(i++));
            entity.setBar(record.getLong(i++));
            return entity;
        });

        var tm = createTransactionManagerOcc(getSession());
        var actualList = tm.executeAndGetList(sql, resultMapping);

        assertFewColumn(actualList);
    }

    @Test
    void fewColumn_getByName() throws Exception {
        var sql = "select * from " + TEST + " order by foo";
        var resultMapping = TgResultMapping.of(record -> {
            var entity = new TestEntity();
            entity.setFoo(record.getInt("foo"));
            entity.setBar(record.getLong("bar"));
            return entity;
        });

        var tm = createTransactionManagerOcc(getSession());
        var actualList = tm.executeAndGetList(sql, resultMapping);

        assertFewColumn(actualList);
    }

    @Test
    void fewColumn_next() throws Exception {
        var sql = "select * from " + TEST + " order by foo";
        var resultMapping = TgResultMapping.of(record -> {
            var entity = new TestEntity();
            entity.setFoo(record.nextInt());
            entity.setBar(record.nextLong());
            return entity;
        });

        var tm = createTransactionManagerOcc(getSession());
        var actualList = tm.executeAndGetList(sql, resultMapping);

        assertFewColumn(actualList);
    }

    private static void assertFewColumn(List<TestEntity> actualList) {
        assertEquals(SIZE, actualList.size());
        int i = 0;
        for (var actual : actualList) {
            assertEquals(i, actual.getFoo());
            assertEquals(i, actual.getBar());
            assertNull(actual.getZzz());
            i++;
        }
    }
}
