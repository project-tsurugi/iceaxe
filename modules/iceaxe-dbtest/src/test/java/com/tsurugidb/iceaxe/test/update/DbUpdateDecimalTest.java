package com.tsurugidb.iceaxe.test.update;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * update decimal test
 */
class DbUpdateDecimalTest extends DbTestTableTester {

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
    @ValueSource(booleans = { false, true })
    void div2(boolean cast) throws Exception {
        div(2, cast, true);
    }

    @Test
    void div3() throws Exception {
        div(3, false, false);
    }

    @Test
    void div3Cast() throws Exception {
        div(3, true, true);
    }

    private void div(int divValue, boolean cast, boolean expectedSuccess) throws IOException, InterruptedException {
        insert(BigDecimal.ONE);

        var div = TgBindVariable.ofDecimal("div");
        String expression = "value / " + div;
        if (cast) {
            expression = "cast(" + expression + " as decimal(5,2))";
        }
        var sql = "update " + TEST + " set value = " + expression;
        var mapping = TgParameterMapping.of(div);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql, mapping)) {
            var parameter = TgBindParameters.of(div.bind(divValue));
            if (expectedSuccess) {
                int count = tm.executeAndGetCount(ps, parameter);
                assertUpdateCount(1, count);

                var actual = selectValue();
                var expected = BigDecimal.ONE.divide(BigDecimal.valueOf(divValue), 2, RoundingMode.DOWN);
                assertEquals(expected, actual);
            } else {
                tm.execute(transaction -> {
                    var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                        transaction.executeAndGetCount(ps, parameter);
                    });
                    assertEqualsCode(SqlServiceCode.ERR_EXPRESSION_EVALUATION_FAILURE, e);

                    try (var selectPs = session.createQuery("select value from " + TEST)) {
                        var list = transaction.executeAndGetList(selectPs);
                        assertEquals(0, list.size()); // TODO 1 (even if ERR_EXPRESSION_EVALUATION_FAILURE)
                    }
                });
            }
        }
    }

    private void insert(BigDecimal value) throws IOException, InterruptedException {
        var variable = TgBindVariable.ofDecimal(VNAME);
        var mapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(SQL, mapping)) {
            var parameter = TgBindParameters.of(variable.bind(value));
            int count = tm.executeAndGetCount(ps, parameter);
            assertUpdateCount(1, count);
        }
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
