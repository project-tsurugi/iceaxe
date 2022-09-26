package com.tsurugidb.iceaxe.test.update;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * update error test
 */
class DbUpdateErrorTest extends DbTestTableTester {

    private static final int SIZE = 10;

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        if (!info.getDisplayName().equals("updateNullToNotNull()")) {
            createTestTable();
            insertTestTable(SIZE);
        }

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void updatePKNull() throws IOException {
        var sql = "update " + TEST //
                + " set" //
                + "  foo = null" // primary key
                + " where foo = 5";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            var e = assertThrows(TsurugiIOException.class, () -> ps.executeAndGetCount(tm));
            assertEqualsCode(null, e); // TODO エラーコード
        }

        assertEqualsTestTable(SIZE);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, SIZE / 2, SIZE * 2 })
    void updatePKSameValue(int newPk) throws IOException {
        var sql = "update " + TEST //
                + " set" //
                + "  foo = " + newPk; // primary key

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            var e = assertThrows(TsurugiIOException.class, () -> ps.executeAndGetCount(tm));
            assertEqualsCode(null, e); // TODO エラーコード
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void updateNullToNotNull() throws IOException {
        var session = getSession();

        var createSql = "create table " + TEST //
                + "(" //
                + "  foo int not null," //
                + "  bar bigint not null," //
                + "  zzz varchar(10) not null," //
                + "  primary key(foo)" //
                + ")";
        executeDdl(session, createSql);
        insertTestTable(SIZE);

        var sql = "update " + TEST //
                + " set" //
                + "  bar = null," //
                + "  zzz = null";

        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            var e = assertThrows(TsurugiIOException.class, () -> {
                ps.executeAndGetCount(tm);
            });
            assertEqualsCode(SqlServiceCode.ERR_INTEGRITY_CONSTRAINT_VIOLATION, e);
            assertTrue(e.getMessage().contains("TODO"), () -> "actual=" + e.getMessage()); // TODO エラー詳細情報の確認
        }

        assertEqualsTestTable(SIZE);
    }
}
