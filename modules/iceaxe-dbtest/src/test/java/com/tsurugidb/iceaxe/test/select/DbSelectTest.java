package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;

/**
 * select all record test
 */
class DbSelectTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll() throws IOException {
        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);
    }

    @Test
    void selectAllByResultEntity() throws IOException {
        var sql = "select * from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            List<TsurugiResultEntity> list = ps.executeAndGetList(tm);
            assertResultEntity(list);
        }
    }

    private static void assertResultEntity(List<TsurugiResultEntity> actualList) {
        assertEquals(SIZE, actualList.size());
        var actualMap = actualList.stream().collect(Collectors.toMap(r -> r.getInt4OrNull("foo"), r -> r));
        for (int i = 0; i < SIZE; i++) {
            var actual = actualMap.get(i);
            var expected = createTestEntity(i);
            assertEquals(expected.getFoo(), actual.getInt4OrNull("foo"));
            assertEquals(expected.getBar(), actual.getInt8OrNull("bar"));
            assertEquals(expected.getZzz(), actual.getCharacterOrNull("zzz"));
        }
    }

    @Test
    void selectAllByTestEntity() throws IOException {
        var sql = "select * from " + TEST;
        var resultMapping = TgResultMapping.of(TestEntity::new) //
                .int4("foo", TestEntity::setFoo) //
                .int8("bar", TestEntity::setBar) //
                .character("zzz", TestEntity::setZzz);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, resultMapping)) {
            List<TestEntity> list = ps.executeAndGetList(tm);
            assertTestEntity(list);
        }
    }

    @Test
    void selectColumnsByTestEntity() throws IOException {
        var sql = "select " + TEST_COLUMNS + " from " + TEST;
        var resultMapping = TgResultMapping.of(TestEntity::new) //
                .int4(TestEntity::setFoo) //
                .int8(TestEntity::setBar) //
                .character(TestEntity::setZzz);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, resultMapping)) {
            List<TestEntity> list = ps.executeAndGetList(tm);
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
        var resultMapping = TgResultMapping.of(record -> record.nextInt4());

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, resultMapping)) {
            List<Integer> list = ps.executeAndGetList(tm);
            assertColumn(list);
        }
    }

    private static void assertColumn(List<Integer> actualList) {
        for (int i = 0; i < SIZE; i++) {
            assertTrue(actualList.contains(i));
        }
    }
}
