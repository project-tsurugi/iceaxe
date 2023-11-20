package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
            assertContains("parsing statement failed: mismatched input '!=' expecting <EOF> (<input>:1:41)", e.getMessage());
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
        var sql = SELECT_SQL + " where bar is null";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            var list = tm.executeAndGetList(ps);
            assertEquals(1, list.size());
            var entity = list.get(0);
            assertEquals(NULL_ENTITY, entity);
        }
    }

    @Test
    void isNotNull() throws Exception {
        var sql = SELECT_SQL + " where bar is not null" + " order by foo";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            var list = tm.executeAndGetList(ps);
            assertEquals(SIZE, list.size());
            for (int i = 0; i < SIZE; i++) {
                assertEquals(i, list.get(i).getFoo());
            }
        }
    }

    @Test
    void isTrue() throws Exception {
        var sql = SELECT_SQL + " where (1=1) is true";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> { // TODO isTrue実装待ち
                var list = tm.executeAndGetList(ps);
                assertEquals(SIZE, list.size());
            });
            assertEqualsCode(SqlServiceCode.UNSUPPORTED_RUNTIME_FEATURE_EXCEPTION, e);
        }
    }

    @Test
    void isFalse() throws Exception {
        var sql = SELECT_SQL + " where (1=0) is false";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> { // TODO isFalse実装待ち
                var list = tm.executeAndGetList(ps);
                assertEquals(SIZE, list.size());
            });
            assertEqualsCode(SqlServiceCode.UNSUPPORTED_RUNTIME_FEATURE_EXCEPTION, e);
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
            assertEqualsCode(SqlServiceCode.SYNTAX_EXCEPTION, e); // TODO in実装待ち
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
            assertEqualsCode(SqlServiceCode.SYNTAX_EXCEPTION, e); // TODO between実装待ち
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
            assertEqualsCode(SqlServiceCode.UNSUPPORTED_RUNTIME_FEATURE_EXCEPTION, e);
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
            assertEqualsCode(SqlServiceCode.UNSUPPORTED_RUNTIME_FEATURE_EXCEPTION, e);
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
