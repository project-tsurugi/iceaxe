package com.tsurugidb.iceaxe.test.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * statement comment test
 */
class DbStatementCommentTest extends DbTestTableTester {

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void comment(boolean prepared) throws IOException {
        var list1 = List.of("", "-- comment1\n", "/*comment1*/");
        var list2 = List.of("", "-- comment2\n", "/*comment2*/");
        var list3 = List.of("", "-- comment3", "-- comment3\n", "/*comment3*/");
        for (var c1 : list1) {
            for (var c2 : list2) {
                for (var c3 : list3) {
                    dropTestTable();
                    createTestTable();

                    var sql = c1 //
                            + "insert into " + TEST + "(foo, bar, zzz)\n" //
                            + c2 //
                            + "values(:foo, :bar, :zzz)\n" //
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
        var entity = createTestEntity(0);
        if (prepared) {
            try (var ps = session.createPreparedStatement(sql, INSERT_MAPPING)) {
                int count = tm.executeAndGetCount(ps, entity);
                assertEquals(-1, count); // TODO 1
            }
        } else {
            var sqlr = sql.replace(":foo", Integer.toString(entity.getFoo())) //
                    .replace(":bar", Long.toString(entity.getBar())) //
                    .replace(":zzz", "'" + entity.getZzz() + "'");
            try (var ps = session.createPreparedStatement(sqlr)) {
                int count = tm.executeAndGetCount(ps);
                assertEquals(-1, count); // TODO 1
            }
        }
        assertEqualsTestTable(1);
    }
}
