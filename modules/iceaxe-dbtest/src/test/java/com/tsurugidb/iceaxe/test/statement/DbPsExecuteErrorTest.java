package com.tsurugidb.iceaxe.test.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

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
class DbPsExecuteErrorTest extends DbTestTableTester {

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
            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
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
            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                var plist = TgParameterList.of(foo.bind(123), bar.bind(456) /* ,zzz */);
                ps.executeAndGetCount(tm, plist);
            });
            assertEqualsCode(SqlServiceCode.ERR_UNRESOLVED_HOST_VARIABLE, e);
            assertContains("Value is not assigned for host variable 'zzz'.", e.getMessage());
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void insertParameterTypeMismatch() throws IOException {
        var foo = TgVariable.ofInt8("foo"); // INT4 <-> Int8
        var bar = TgVariable.ofInt4("bar"); // INT8 <-> Int4
        var zzz = TgVariable.ofInt4("zzzi"); // CHARACTER <-> Int4
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(" + foo + ", " + bar + ", " + zzz + ")";
        var parameterMapping = TgParameterMapping.of(foo, bar, zzz);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
            var e = assertThrowsExactly(TsurugiIOException.class, () -> {
                var plist = TgParameterList.of(foo.bind(123), bar.bind(456), zzz.bind(789));
                ps.executeAndGetCount(tm, plist);
            });
            assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
            assertContains("inconsistent_type int4() (expected: {character_string})", e.getMessage());; // TODO カラム名の確認
        }

        assertEqualsTestTable(SIZE);
    }
}
