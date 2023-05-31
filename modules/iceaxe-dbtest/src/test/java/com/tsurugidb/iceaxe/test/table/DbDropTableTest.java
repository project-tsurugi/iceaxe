package com.tsurugidb.iceaxe.test.table;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * drop table test
 */
class DbDropTableTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();

        logInitEnd(info);
    }

    private static final String SQL = "drop table " + TEST;

    @Test
    void drop() throws Exception {
        createTestTable();

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(SQL)) {
            tm.executeAndGetCount(ps);
        }
    }

    @Test
    void dropNotFound() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(SQL)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                tm.executeAndGetCount(ps);
            });
            assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
            assertContains("table_not_found table `test' is not found", e.getMessage());
        }
    }

    @Test
    void rollback() throws Exception {
        createTestTable();

        var session = getSession();
        assertTrue(session.findTableMetadata(TEST).isPresent());

        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(SQL)) {
            tm.execute(transaction -> {
                transaction.executeAndGetCount(ps);
                assertTrue(session.findTableMetadata(TEST).isEmpty());
                transaction.rollback();
                assertTrue(session.findTableMetadata(TEST).isEmpty());
            });
        }

        assertTrue(session.findTableMetadata(TEST).isEmpty());
    }
}
