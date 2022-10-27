package com.tsurugidb.iceaxe.test.table;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * create table test
 */
class DbCreateTableTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    private static final String SQL = "create table " + TEST //
            + "(" //
            + "  foo int," //
            + "  bar bigint," //
            + "  zzz varchar(10)," //
            + "  primary key(foo)" //
            + ")";

    @Test
    void create() throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(SQL)) {
            ps.executeAndGetCount(tm);
        }
    }

    @Test
    void createExists() throws IOException {
        createTestTable();

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(SQL)) {
            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                ps.executeAndGetCount(tm);
            });
            assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
            assertContains("duplicate_table table `test' is already defined.", e.getMessage());
        }
    }

    @Test
    void rollback() throws IOException {
        var session = getSession();
        assertTrue(session.findTableMetadata(TEST).isEmpty());

        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(SQL)) {
            tm.execute(transaction -> {
                ps.executeAndGetCount(transaction);
                assertTrue(session.findTableMetadata(TEST).isPresent());
                transaction.rollback();
                assertTrue(session.findTableMetadata(TEST).isPresent());
            });
        }

        assertTrue(session.findTableMetadata(TEST).isPresent());
    }
}
