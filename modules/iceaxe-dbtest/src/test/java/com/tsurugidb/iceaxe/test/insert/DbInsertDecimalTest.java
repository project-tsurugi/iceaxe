package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.math.BigDecimal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.statement.TgVariable.TgVariableBigDecimal;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * insert decimal test
 */
class DbInsertDecimalTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    private static void createTable() throws IOException {
        var sql = "create table " + TEST //
                + "(" //
                + "  value decimal(5,2)" //
                + ")";
        executeDdl(getSession(), sql);
    }

    private static final String VNAME = "value";
    private static final String SQL = "insert into " + TEST + "(value) values(:" + VNAME + ")";

    @ParameterizedTest
    @ValueSource(strings = { "123", "1.01" })
    void normal(String s) throws IOException {
        var value = new BigDecimal(s);
        var variable = TgVariable.ofDecimal(VNAME);
        var mapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(SQL, mapping)) {
            var parameter = TgParameterList.of(variable.bind(value));
            int count = ps.executeAndGetCount(tm, parameter);
            assertEquals(-1, count); // TODO 1
        }

        try (var ps = session.createPreparedQuery("select value from " + TEST)) {
            var entity = ps.executeAndFindRecord(tm).get();
            var actual = entity.getDecimal("value");
            assertEquals(value.setScale(2), actual);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "1234", "1.001" })
    void outOfRange(String s) throws IOException {
        var value = new BigDecimal(s);
        var variable = TgVariable.ofDecimal(VNAME);
        var mapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(SQL, mapping)) {
            var parameter = TgParameterList.of(variable.bind(value));
            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> ps.executeAndGetCount(tm, parameter));
            assertEqualsCode(SqlServiceCode.ERR_EXPRESSION_EVALUATION_FAILURE, e);
            assertContains("TODO", e.getMessage()); // TODO エラー詳細情報の確認
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "1.1", "1.01", "1.001" })
    void scale(String s) throws IOException {
        var value = new BigDecimal(s);
        var variable = TgVariable.ofDecimal(VNAME, 2); // TgVariable with scale
        var mapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(SQL, mapping)) {
            var parameter = TgParameterList.of(variable.bind(value));
            int count = ps.executeAndGetCount(tm, parameter);
            assertEquals(-1, count); // TODO 1
        }

        try (var ps = session.createPreparedQuery("select value from " + TEST)) {
            var entity = ps.executeAndFindRecord(tm).get();
            var actual = entity.getDecimal("value");
            var expected = value.setScale(2, TgVariableBigDecimal.DEFAULT_ROUNDING_MODE);
            assertEquals(expected, actual);
        }
    }
}
