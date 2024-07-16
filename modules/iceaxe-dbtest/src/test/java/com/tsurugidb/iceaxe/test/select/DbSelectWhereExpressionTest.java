package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    private static final int SIZE = 20;
    private static TestEntity NULL_ENTITY = new TestEntity(123, null, null);

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
        var sql = SELECT_SQL + " where foo != 1";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                tm.executeAndGetList(ps);
            });
            assertEqualsCode(SqlServiceCode.SYNTAX_EXCEPTION, e);
            assertContains("unrecognized character: \"!\"", e.getMessage());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "=", "<>", "<", ">", "<=", ">=" })
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
        var expectedList = List.of(2, 4, 5, 7);
        var sql = SELECT_SQL + " where foo in (" + expectedList.stream().map(n -> Integer.toString(n)).collect(Collectors.joining(",")) + ")";
//      var sql = SELECT_SQL + " where " + expectedList.stream().map(n -> "foo=" + n).collect(Collectors.joining(" or ")) ;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                var list = tm.executeAndGetList(ps);
                assertWhere(entity -> expectedList.contains(entity.getFoo()), list);
            });
            assertEqualsCode(SqlServiceCode.UNSUPPORTED_COMPILER_FEATURE_EXCEPTION, e); // TODO in実装待ち
        }
    }

    @Test
    void between() throws Exception {
        int start = 4;
        int end = 6;
        var sql = SELECT_SQL + " where foo between " + start + " and " + end;
//      var sql = SELECT_SQL + " where " + start + " <= foo and foo <= " + end;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                var list = tm.executeAndGetList(ps);
                assertWhere(entity -> start <= entity.getFoo() && entity.getFoo() <= end, list);
            });
            assertEqualsCode(SqlServiceCode.UNSUPPORTED_COMPILER_FEATURE_EXCEPTION, e); // TODO between実装待ち
        }
    }

    @Test
    void like() throws Exception {
        var sql = SELECT_SQL + " where zzz like'1%'";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> { // // TODO like実装待ち
                var list = tm.executeAndGetList(ps);
                assertWhere(entity -> entity.getZzz().startsWith("1"), list);
            });
            assertEqualsCode(SqlServiceCode.UNSUPPORTED_COMPILER_FEATURE_EXCEPTION, e);
        }
    }

    @Test
    void notLike() throws Exception {
        var sql = SELECT_SQL + " where zzz not like'1%'";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> { // // TODO like実装待ち
                var list = tm.executeAndGetList(ps);
                assertWhere(entity -> !entity.getZzz().startsWith("1"), list);
            });
            assertEqualsCode(SqlServiceCode.UNSUPPORTED_COMPILER_FEATURE_EXCEPTION, e);
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
        return map;
    }
}
