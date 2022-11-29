package com.tsurugidb.iceaxe.test.table;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * create table error test
 */
class DbCreateTableErrorTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void columnNotFoundPk() throws IOException {
        var sql = "create table " + TEST //
                + "(" //
                + "  foo int," //
                + "  bar bigint," //
                + "  zzz varchar(10)," //
                + "  primary key(goo)" //
                + ")";
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                ps.executeAndGetCount(tm);
            });
            assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
            assertContains("translating statement failed: column_not_found primary key column \"goo\" is not found", e.getMessage());
        }
    }

    @Test
    void duplicatePkDefinition() throws IOException {
        var sql = "create table " + TEST //
                + "(" //
                + "  foo int primary key," //
                + "  bar bigint," //
                + "  zzz varchar(10)," //
                + "  primary key(foo)" //
                + ")";
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                ps.executeAndGetCount(tm);
            });
            assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
            assertContains("translating statement failed: invalid_default_value primary key definition must be upto one", e.getMessage());
        }
    }

    @Test
    void duplicatePk() throws IOException {
        var sql = "create table " + TEST //
                + "(" //
                + "  foo int," //
                + "  bar bigint," //
                + "  zzz varchar(10)," //
                + "  primary key(foo, foo)" //
                + ")";
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            // TODO cause exception
            ps.executeAndGetCount(tm);
        }
    }

    @Test
    void duplicateColumnName() throws IOException {
        for (int i = 0; i <= 0b11; i++) {
            var sql = getCreateSql(i);
            if (sql == null) {
                continue;
            }
            try {
                test(sql);
            } catch (Throwable e) {
                LOG.error("duplicateColumnName fail. ddl={}", sql, e);
                throw e;
            }
        }
    }

    void test(String sql) throws IOException {
        dropTestTable();

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            // TODO cause exception
            ps.executeAndGetCount(tm);
        }
    }

    private static String getCreateSql(int pk) {
        boolean pk1 = (pk & 0b1) != 0;
        boolean pk2 = (pk & 0b10) != 0;
        if (pk1 && pk2) {
            return null;
        }

        return "create table " + TEST //
                + "(" //
                + "  foo int " + (pk1 ? " primary key" : "") + "," //
                + "  foo bigint" //
                + (pk2 ? ",primary key(foo)" : "") //
                + ")";
    }
}
