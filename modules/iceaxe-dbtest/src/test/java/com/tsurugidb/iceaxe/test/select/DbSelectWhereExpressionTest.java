package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * select where expression test
 */
class DbSelectWhereExpressionTest extends DbTestTableTester {

    private static final int SIZE = 10;
    private static TestEntity NULL_ENTITY = new TestEntity(123, null, null);

    @BeforeAll
    static void beforeAll() throws IOException {
        var LOG = LoggerFactory.getLogger(DbSelectWhereExpressionTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);
        insertTestTable(NULL_ENTITY);

        LOG.debug("init end");
    }

    @Test
    void invalidExpression() throws IOException {
        var sql = SELECT_SQL + " where foo != 1";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            var e = assertThrowsExactly(TsurugiIOException.class, () -> ps.executeAndGetList(tm));
            assertEqualsCode(SqlServiceCode.ERR_PARSE_ERROR, e);
            assertContains("TODO", e.getMessage()); // TODO エラー詳細情報の確認
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "=", "<>", "<", ">", "<=", ">=" })
    void eqNull(String expression) throws IOException {
        var sql = SELECT_SQL + " where bar " + expression + " null";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            var list = ps.executeAndGetList(tm);
            assertEquals(0, list.size());
        }
    }

    @Test
    void isNull() throws IOException {
        var sql = SELECT_SQL + " where bar is null";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, SELECT_MAPPING)) {
            var list = ps.executeAndGetList(tm);
            assertEquals(1, list.size());
            var entity = list.get(0);
            assertEquals(NULL_ENTITY, entity);
        }
    }

    @Test
    void isNotNull() throws IOException {
        var sql = SELECT_SQL + " where bar is not null" + " order by foo";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, SELECT_MAPPING)) {
            var list = ps.executeAndGetList(tm);
            assertEquals(SIZE, list.size());
            for (int i = 0; i < SIZE; i++) {
                assertEquals(i, list.get(i).getFoo());
            }
        }
    }

    @Test
    void in() throws IOException {
        var expectedList = List.of(2, 4, 5, 7);
        var sql = SELECT_SQL + " where foo in (" + expectedList.stream().map(n -> Integer.toString(n)).collect(Collectors.joining(",")) + ") order by foo";
//      var sql = SELECT_SQL + " where " + expectedList.stream().map(n -> "foo=" + n).collect(Collectors.joining(" or ")) + " order by foo";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, SELECT_MAPPING)) {
            var list = ps.executeAndGetList(tm);
            assertEquals(expectedList.size(), list.size());
            for (int i = 0; i < list.size(); i++) {
                assertEquals(expectedList.get(i), list.get(i).getFoo());
            }
        }
    }

    @Test
    void between() throws IOException {
        int s = 4;
        int e = 6;
        var sql = SELECT_SQL + " where foo between " + s + " and " + e + " order by foo";
//      var sql = SELECT_SQL + " where " + s + " <= foo and foo <= " + e + " order by foo";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, SELECT_MAPPING)) {
            var list = ps.executeAndGetList(tm);
            assertEquals(e - s + 1, list.size());
            for (int i = 0; i < list.size(); i++) {
                assertEquals(s + i, list.get(i).getFoo());
            }
        }
    }
}
