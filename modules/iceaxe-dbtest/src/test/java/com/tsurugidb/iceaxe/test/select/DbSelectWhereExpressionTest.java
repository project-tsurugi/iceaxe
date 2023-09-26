package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.util.List;
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

    private static final int SIZE = 10;
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
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                var list = tm.executeAndGetList(ps);
                assertEquals(1, list.size());
                var entity = list.get(0);
                assertEquals(NULL_ENTITY, entity);
            });
            assertEqualsCode(SqlServiceCode.SYNTAX_EXCEPTION, e); // TODO is null実装待ち
        }
    }

    @Test
    void isNotNull() throws Exception {
        var sql = SELECT_SQL + " where bar is not null" + " order by foo";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                var list = tm.executeAndGetList(ps);
                assertEquals(SIZE, list.size());
                for (int i = 0; i < SIZE; i++) {
                    assertEquals(i, list.get(i).getFoo());
                }
            });
            assertEqualsCode(SqlServiceCode.SYNTAX_EXCEPTION, e); // TODO is not null実装待ち
        }
    }

    @Test
    void in() throws Exception {
        var expectedList = List.of(2, 4, 5, 7);
        var sql = SELECT_SQL + " where foo in (" + expectedList.stream().map(n -> Integer.toString(n)).collect(Collectors.joining(",")) + ") order by foo";
//      var sql = SELECT_SQL + " where " + expectedList.stream().map(n -> "foo=" + n).collect(Collectors.joining(" or ")) + " order by foo";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                var list = tm.executeAndGetList(ps);
                assertEquals(expectedList.size(), list.size());
                for (int i = 0; i < list.size(); i++) {
                    assertEquals(expectedList.get(i), list.get(i).getFoo());
                }
            });
            assertEqualsCode(SqlServiceCode.SYNTAX_EXCEPTION, e); // TODO in実装待ち
        }
    }

    @Test
    void between() throws Exception {
        int s = 4;
        int e = 6;
        var sql = SELECT_SQL + " where foo between " + s + " and " + e + " order by foo";
//      var sql = SELECT_SQL + " where " + s + " <= foo and foo <= " + e + " order by foo";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            var e0 = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                var list = tm.executeAndGetList(ps);
                assertEquals(e - s + 1, list.size());
                for (int i = 0; i < list.size(); i++) {
                    assertEquals(s + i, list.get(i).getFoo());
                }
            });
            assertEqualsCode(SqlServiceCode.SYNTAX_EXCEPTION, e0); // TODO between実装待ち
        }
    }
}
