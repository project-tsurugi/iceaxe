package com.tsurugidb.iceaxe.test.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * query comment test
 */
class DbQueryCommentTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll() throws IOException {
        var LOG = LoggerFactory.getLogger(DbQueryCommentTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("init end");
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void comment(boolean prepared) throws IOException {
        var list1 = List.of("", "-- comment1\n", "/*comment1*/");
        var list2 = List.of("", "-- comment2\n", "/*comment2*/");
        var list3 = List.of("", "-- comment3", "-- comment3\n", "/*comment3*/");
        for (var c1 : list1) {
            for (var c2 : list2) {
                for (var c3 : list3) {
                    var sql = c1 //
                            + "select * from " + TEST + "\n" //
                            + c2 //
                            + "order by foo\n" //
                            + c3;
                    try {
                        test(sql, prepared);
                    } catch (Throwable e) {
                        LOG.error("sql=[{}]", sql, e);
                        throw e;
                    }
                }
            }
        }
    }

    private void test(String sql, boolean prepared) throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        if (prepared) {
            try (var ps = session.createPreparedQuery(sql, SELECT_MAPPING)) {
                var list = tm.executeAndGetList(ps);
                assertEquals(SIZE, list.size());
                for (int i = 0; i < SIZE; i++) {
                    var expected = createTestEntity(i);
                    var actual = list.get(i);
                    assertEquals(expected, actual);
                }
            }
        } else {
            try (var ps = session.createPreparedQuery(sql)) {
                var list = tm.executeAndGetList(ps);
                assertEquals(SIZE, list.size());
                for (int i = 0; i < SIZE; i++) {
                    var expected = createTestEntity(i);
                    var actual = list.get(i);
                    assertEquals(expected.getFoo(), actual.getInt4("foo"));
                    assertEquals(expected.getBar(), actual.getInt8("bar"));
                    assertEquals(expected.getZzz(), actual.getCharacter("zzz"));
                }
            }
        }
    }
}
