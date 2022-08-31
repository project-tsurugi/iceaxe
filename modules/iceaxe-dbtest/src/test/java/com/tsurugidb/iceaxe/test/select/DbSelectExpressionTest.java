package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * select expression test
 */
class DbSelectExpressionTest extends DbTestTableTester {

    private static final int SIZE = 10;

    @BeforeAll
    static void beforeAll() throws IOException {
        var LOG = LoggerFactory.getLogger(DbSelectExpressionTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("init end");
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
