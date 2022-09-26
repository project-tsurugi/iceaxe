package com.tsurugidb.iceaxe.test.error;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * PreparedStatement execute error test
 */
class DbErrorPsExecuteTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("{} end start", info.getDisplayName());
    }

    @Test
    void intsertByExecuteQuery() throws IOException {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(123, 456, 'abc')";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            var e = assertThrows(TsurugiTransactionIOException.class, () -> {
                ps.executeAndGetList(tm);
            });
            assertEqualsCode(SqlServiceCode.ERR_ILLEGAL_OPERATION, e);
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void selectByExecuteStatement() throws IOException {
        var sql = "select * from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            int count = ps.executeAndGetCount(tm);
            assertEquals(-1, count); // TODO 0?
        }
    }

    @Test
    void insertParameterSizeUnmatch() throws IOException {
        var foo = TgVariable.ofInt4("foo");
        var bar = TgVariable.ofInt8("bar");
        var zzz = TgVariable.ofCharacter("zzz");
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(" + foo + ", " + bar + ", " + zzz + ")";
        var parameterMapping = TgParameterMapping.of(foo, bar, zzz);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
            var e = assertThrows(TsurugiTransactionIOException.class, () -> {
                var plist = TgParameterList.of(foo.bind(123), bar.bind(456) /* ,zzz */);
                ps.executeAndGetCount(tm, plist);
            });
            assertEqualsCode(SqlServiceCode.ERR_UNRESOLVED_HOST_VARIABLE, e);
            assertTrue(e.getMessage().contains("Value is not assigned for host variable 'zzz'."), () -> "actual=" + e.getMessage());
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void insertParameterTypeUnmatch() throws IOException {
        var foo = TgVariable.ofInt8("foo"); // INT4
        var bar = TgVariable.ofInt4("bar"); // INT8
        var zzz = TgVariable.ofInt4("zzz"); // CHARACTER
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(" + foo + ", " + bar + ", " + zzz + ")";
        var parameterMapping = TgParameterMapping.of(foo, bar, zzz);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
            var e = assertThrows(TsurugiIOException.class, () -> {
                var plist = TgParameterList.of(foo.bind(123), bar.bind(456), zzz.bind(789));
                ps.executeAndGetCount(tm, plist);
            });
            assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
            assertTrue(e.getMessage().contains("TODO"), () -> "actual=" + e.getMessage()); // TODO エラー詳細情報の確認
        }

        assertEqualsTestTable(SIZE);
    }
}
