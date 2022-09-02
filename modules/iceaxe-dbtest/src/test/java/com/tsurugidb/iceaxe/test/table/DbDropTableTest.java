package com.tsurugidb.iceaxe.test.table;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * drop table test
 */
class DbDropTableTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    private static final String SQL = "drop table " + TEST;

    @Test
    void drop() throws IOException {
        createTestTable();

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(SQL)) {
            ps.executeAndGetCount(tm);
        }
    }

    @Test
    void dropNotFound() throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(SQL)) {
            var e = assertThrows(TsurugiTransactionIOException.class, () -> {
                ps.executeAndGetCount(tm);
            });
            assertEqualsCode(SqlServiceCode.ERR_TRANSLATOR_ERROR, e); // TODO table_not_found
        }
    }

    @Test
    void rollback() throws IOException {
        createTestTable();

        var session = getSession();
        assertTrue(session.findTableMetadata(TEST).isPresent());

        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(SQL)) {
            tm.execute(transaction -> {
                ps.executeAndGetCount(transaction);
                assertTrue(session.findTableMetadata(TEST).isEmpty());
                transaction.rollback();
                assertTrue(session.findTableMetadata(TEST).isEmpty());
            });
        }

        assertTrue(session.findTableMetadata(TEST).isEmpty());
    }
}
