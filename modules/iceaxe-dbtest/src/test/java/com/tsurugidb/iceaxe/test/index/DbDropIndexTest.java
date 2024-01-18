package com.tsurugidb.iceaxe.test.index;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * drop index test
 */
class DbDropIndexTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        executeDdl(getSession(), "create index idx_test_bar on " + TEST + " (bar)");

        logInitEnd(info);
    }

    @Test
    void drop() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);

        var sql = "drop index idx_test_bar";
        tm.executeDdl(sql);

        var e = assertThrows(TsurugiTmIOException.class, () -> {
            tm.executeDdl(sql);
        });
        assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e);
        assertContains("index `idx_test_bar' is not found", e.getMessage());
    }

    @Test
    void dropTable() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);

        tm.executeDdl("drop table " + TEST);

        var e = assertThrows(TsurugiTmIOException.class, () -> {
            tm.executeDdl("drop index idx_test_bar");
        });
        assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e);
        assertContains("index `idx_test_bar' is not found", e.getMessage());
    }
}
