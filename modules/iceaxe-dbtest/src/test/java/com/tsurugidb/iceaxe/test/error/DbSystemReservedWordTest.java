package com.tsurugidb.iceaxe.test.error;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * system reserved word test
 *
 * <ul>
 * <li>アンダースコア2個で始まるテーブル名やカラム名はシステムで予約されており、ユーザーは使用することが出来ない。</li>
 * </ul>
 */
class DbSystemReservedWordTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();

        logInitEnd(info);
    }

    @Test
    void createTable() throws Exception {
        String tableName = "__test";
        dropTable(tableName);
        var sql = "create table " + tableName //
                + "(" //
                + "  foo int," //
                + "  bar bigint," //
                + "  zzz varchar(10)" //
                + ")";
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            executeDdl(getSession(), sql, tableName);
        });
        assertErrorSystemReservedWord(tableName, e);
    }

    @Test
    void createTableColumn() throws Exception {
        var sql = "create table " + TEST //
                + "(" //
                + "  __foo int," //
                + "  __bar bigint," //
                + "  __zzz varchar(10)" //
                + ")";
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            executeDdl(getSession(), sql);
        });
        assertErrorSystemReservedWord("__foo", e);
    }

    @Test
    void selectAs() throws Exception {
        int size = 4;
        createTestTable();
        insertTestTable(size);

        var sql = "select foo as __foo from " + TEST + " order by foo";
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                tm.executeAndGetList(ps);
            });
            assertErrorSystemReservedWord("__foo", e);
        }
    }

    private static void assertErrorSystemReservedWord(String expected, Exception actual) {
        assertEqualsCode(SqlServiceCode.SYNTAX_EXCEPTION, actual);
        assertContains("identifier must not start with two underscores: " + expected, actual.getMessage());
    }
}
