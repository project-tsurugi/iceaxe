package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.math.BigDecimal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableBigDecimal;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * insert decimal test
 */
class DbInsertDecimalTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTable();

        logInitEnd(info);
    }

    private static void createTable() throws IOException, InterruptedException {
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
    void normal(String s) throws Exception {
        var value = new BigDecimal(s);
        var variable = TgBindVariable.ofDecimal(VNAME);
        var mapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(SQL, mapping)) {
            var parameter = TgBindParameters.of(variable.bind(value));
            int count = tm.executeAndGetCount(ps, parameter);
            assertUpdateCount(1, count);
        }

        var actual = selectValue();
        assertEquals(value.setScale(2), actual);
    }

    @ParameterizedTest
    @ValueSource(strings = { "1234", "1.001" })
    void outOfRange(String s) throws Exception {
        var value = new BigDecimal(s);
        var variable = TgBindVariable.ofDecimal(VNAME);
        var mapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(SQL, mapping)) {
            var parameter = TgBindParameters.of(variable.bind(value));
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> tm.executeAndGetCount(ps, parameter));
            assertEqualsCode(SqlServiceCode.ERR_EXPRESSION_EVALUATION_FAILURE, e);
            String expected = "ERR_EXPRESSION_EVALUATION_FAILURE: SQL--0017:";
            assertContains(expected, e.getMessage()); // TODO エラー詳細情報の確認
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "1.1", "1.01", "1.001" })
    void scale(String s) throws Exception {
        var value = new BigDecimal(s);
        var variable = TgBindVariable.ofDecimal(VNAME, 2); // TgVariable with scale
        var mapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(SQL, mapping)) {
            var parameter = TgBindParameters.of(variable.bind(value));
            int count = tm.executeAndGetCount(ps, parameter);
            assertUpdateCount(1, count);
        }

        var actual = selectValue();
        var expected = value.setScale(2, TgBindVariableBigDecimal.DEFAULT_ROUNDING_MODE);
        assertEquals(expected, actual);
    }

    private BigDecimal selectValue() throws IOException, InterruptedException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery("select value from " + TEST)) {
            var entity = tm.executeAndFindRecord(ps).get();
            return entity.getDecimal(VNAME);
        }
    }
}
