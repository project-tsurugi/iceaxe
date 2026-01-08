package com.tsurugidb.iceaxe.test.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * coalesce function test
 */
class DbCoalesceTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(LOG, info);

        dropTestTable();
        createTable();
        insertTable();

        logInitEnd(LOG, info);
    }

    private static void createTable() throws IOException, InterruptedException {
        var sql = "create table " + TEST + "(" //
                + " foo int primary key," //
                + " bar1 bigint," //
                + " bar2 bigint," //
                + " bar3 bigint" //
                + ")";
        executeDdl(getSession(), sql, TEST);
    }

    private static void insertTable() throws IOException, InterruptedException {
        var tm = createTransactionManagerOcc(getSession());
        for (int i = 0; i <= 0b111; i++) {
            Integer bar1 = bar1IsNull(i) ? null : 11;
            Integer bar2 = bar2IsNull(i) ? null : 22;
            Integer bar3 = bar3IsNull(i) ? null : 33;
            var sql = String.format("insert or replace into " + TEST + " values(%d, %d, %d, %d)", //
                    i, bar1, bar2, bar3);
            tm.executeAndGetCountDetail(sql);
        }
    }

    private static boolean bar1IsNull(int i) {
        return (i & 0b001) != 0;
    }

    private static boolean bar2IsNull(int i) {
        return (i & 0b010) != 0;
    }

    private static boolean bar3IsNull(int i) {
        return (i & 0b100) != 0;
    }

    @Test
    void test() throws Exception {
        var sql = "select foo, coalesce(bar1, bar2, bar3, 999) from " + TEST;

        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql);
        assertEquals(8, list.size());

        for (var entity : list) {
            int foo = entity.getInt("foo");
            if (!bar1IsNull(foo)) {
                assertEquals(11, entity.getLong(1));
            } else if (!bar2IsNull(foo)) {
                assertEquals(22, entity.getLong(1));
            } else if (!bar3IsNull(foo)) {
                assertEquals(33, entity.getLong(1));
            } else {
                assertEquals(999, entity.getLong(1));
            }
        }
    }

    @Test
    void test2() throws Exception {
        var sql = "select foo, coalesce(bar1, bar2, bar3) from " + TEST;

        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql);
        assertEquals(8, list.size());

        for (var entity : list) {
            int foo = entity.getInt("foo");
            if (!bar1IsNull(foo)) {
                assertEquals(11, entity.getLong(1));
            } else if (!bar2IsNull(foo)) {
                assertEquals(22, entity.getLong(1));
            } else if (!bar3IsNull(foo)) {
                assertEquals(33, entity.getLong(1));
            } else {
                assertNull(entity.getLongOrNull(1));
            }
        }
    }

    @Test
    void testNull() throws Exception {
        assertNull(longFunction("coalesce(null)"));
        assertNull(longFunction("coalesce(null, null)"));
    }

    private Long longFunction(String function) throws IOException, InterruptedException {
        var sql = "select " + function + " from " + TEST + " limit 1";
        var resultMapping = TgResultMapping.ofSingle(Long.class);

        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql, resultMapping);
        return list.get(0);
    }

}
