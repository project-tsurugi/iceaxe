package com.tsurugidb.iceaxe.test.table;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.exception.SqlServiceCode;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;

/**
 * drop table test
 */
class DbDropTableTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach() throws IOException {
        dropTestTable();
    }

    @Test
    void drop() throws IOException {
        createTestTable();

        var sql = "drop table " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            ps.executeAndGetCount(tm);
        }
    }

    @Test
    void dropNotFound() throws IOException {
        var sql = "drop table " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            var e = assertThrows(TsurugiTransactionIOException.class, () -> {
                ps.executeAndGetCount(tm);
            });
            assertEqualsCode(SqlServiceCode.ERR_TRANSLATOR_ERROR, e); // TODO table_not_found
        }
    }
}
