package com.tsurugidb.iceaxe.test.sql;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * PreparedStatement execute error test
 */
class DbPsExecuteErrorTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("{} end start", info.getDisplayName());
    }

    @Test
    void intsertByExecuteQuery() throws Exception {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(123, 456, 'abc')";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                tm.executeAndGetList(ps);
            });
            assertEqualsCode(SqlServiceCode.ERR_ILLEGAL_OPERATION, e);
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void selectByExecuteStatement() throws Exception {
        var sql = "select * from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            int count = tm.executeAndGetCount(ps);
            assertUpdateCount(0, count); // TODO 0?
        }
    }

    @Test
    void insertParameterSizeUnmatch() throws Exception {
        var foo = TgBindVariable.ofInt("foo");
        var bar = TgBindVariable.ofLong("bar");
        var zzz = TgBindVariable.ofString("zzz");
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(" + foo + ", " + bar + ", " + zzz + ")";
        var parameterMapping = TgParameterMapping.of(foo, bar, zzz);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql, parameterMapping)) {
            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                var parameter = TgBindParameters.of(foo.bind(123), bar.bind(456) /* ,zzz */);
                tm.executeAndGetCount(ps, parameter);
            });
            assertEqualsCode(SqlServiceCode.ERR_UNRESOLVED_HOST_VARIABLE, e);
            assertContains("Value is not assigned for host variable 'zzz'.", e.getMessage());
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void insertParameterTypeMismatch() throws Exception {
        var foo = TgBindVariable.ofLong("foo"); // INT4 <-> Int8
        var bar = TgBindVariable.ofInt("bar"); // INT8 <-> Int4
        var zzz = TgBindVariable.ofInt("zzzi"); // CHARACTER <-> Int4
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(" + foo + ", " + bar + ", " + zzz + ")";
        var parameterMapping = TgParameterMapping.of(foo, bar, zzz);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql, parameterMapping)) {
            var e = assertThrowsExactly(TsurugiIOException.class, () -> {
                var parameter = TgBindParameters.of(foo.bind(123), bar.bind(456), zzz.bind(789));
                tm.executeAndGetCount(ps, parameter);
            });
            assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
            assertContains("inconsistent_type int4() (expected: {character_string})", e.getMessage()); // TODO カラム名の確認
        }

        assertEqualsTestTable(SIZE);
    }
}
