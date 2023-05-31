package com.tsurugidb.iceaxe.test.table;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * create table error test
 */
class DbCreateTableErrorTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();

        logInitEnd(info);
    }

    @Test
    void columnNotFoundPk() throws Exception {
        var sql = "create table " + TEST //
                + "(" //
                + "  foo int," //
                + "  bar bigint," //
                + "  zzz varchar(10)," //
                + "  primary key(goo)" //
                + ")";
        var e = executeErrorDdl(sql);
        assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
        assertContains("translating statement failed: column_not_found primary key column \"goo\" is not found", e.getMessage());
    }

    @Test
    void duplicatePkDefinition() throws Exception {
        var sql = "create table " + TEST //
                + "(" //
                + "  foo int primary key," //
                + "  bar bigint," //
                + "  zzz varchar(10)," //
                + "  primary key(foo)" //
                + ")";
        var e = executeErrorDdl(sql);
        assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
        assertContains("translating statement failed: invalid_default_value primary key definition must be upto one", e.getMessage());
    }

    @Test
    void duplicatePk() throws Exception {
        var sql = "create table " + TEST //
                + "(" //
                + "  foo int," //
                + "  bar bigint," //
                + "  zzz varchar(10)," //
                + "  primary key(foo, foo)" //
                + ")";
        // TODO executeErrorDdl(sql)
        executeDdl(getSession(), sql);
    }

    @Test
    void duplicateColumnName() throws Exception {
        for (int i = 0; i <= 0b11; i++) {
            var sql = getDuplicateColumnSql(i);
            if (sql == null) {
                continue;
            }

            dropTestTable();
            try {
                // TODO executeErrorDdl(sql)
                executeDdl(getSession(), sql);
            } catch (Throwable e) {
                LOG.error("duplicateColumnName fail. ddl={}", sql, e);
                throw e;
            }
        }
    }

    private static String getDuplicateColumnSql(int pk) {
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

    private static TsurugiTmIOException executeErrorDdl(String sql) throws IOException {
        var tm = createTransactionManagerOcc(getSession());
        return assertThrowsExactly(TsurugiTmIOException.class, () -> {
            tm.executeDdl(sql);
        });
    }
}
