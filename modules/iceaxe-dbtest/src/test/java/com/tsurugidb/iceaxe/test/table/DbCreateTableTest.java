package com.tsurugidb.iceaxe.test.table;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.exception.SqlServiceCode;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;

/**
 * create table test
 */
class DbCreateTableTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach() throws IOException {
        dropTestTable();
    }

    @Test
    void create() throws IOException {
        var sql = "create table " + TEST //
                + "(" //
                + "  foo int," //
                + "  bar bigint," //
                + "  zzz varchar(10)," //
                + "  primary key(foo)" //
                + ")";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            ps.executeAndGetCount(tm);
        }
    }

    @Test
    void createExists() throws IOException {
        createTestTable();

        var sql = "create table " + TEST //
                + "(" //
                + "  foo int," //
                + "  bar bigint," //
                + "  zzz varchar(10)," //
                + "  primary key(foo)" //
                + ")";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            var e = assertThrows(TsurugiTransactionIOException.class, () -> {
                ps.executeAndGetCount(tm);
            });
            assertEqualsCode(SqlServiceCode.ERR_TRANSLATOR_ERROR, e); // TODO duplicate_table
        }
    }
}
