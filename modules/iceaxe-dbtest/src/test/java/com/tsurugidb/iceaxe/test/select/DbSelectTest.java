package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
    static void beforeAll() throws IOException {
        var LOG = LoggerFactory.getLogger(DbSelectTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("init end");
    }

    @Test
    void selectAllByResultEntity() throws IOException {
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
        }
    }

    @Test
    void selectAllByTestEntity() throws IOException {
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
    void selectColumnsByTestEntity() throws IOException {
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
    void selectColumn() throws IOException {
        var sql = "select foo from " + TEST;
        var resultMapping = TgResultMapping.of(record -> record.nextInt());

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, resultMapping)) {
            List<Integer> list = tm.executeAndGetList(ps);
            assertColumn(list);
        }
    }

    private static void assertColumn(List<Integer> actualList) {
        for (int i = 0; i < SIZE; i++) {
            assertTrue(actualList.contains(i));
        }
    }
}
