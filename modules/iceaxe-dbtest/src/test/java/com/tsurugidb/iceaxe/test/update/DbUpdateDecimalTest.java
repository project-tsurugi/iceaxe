package com.tsurugidb.iceaxe.test.update;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * update decimal test
 */
class DbUpdateDecimalTest extends DbTestTableTester {

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

    @Test
    void div2() throws IOException {
        div(2, true);
    }

    @Test
    void div3() throws IOException {
        div(3, false); // TODO 1/3も正常にupdateしたい
    }

    private void div(int divValue, boolean success) throws IOException {
        insert(BigDecimal.ONE);

        var div = TgVariable.ofDecimal("div");
        var sql = "update " + TEST + " set value = value / " + div;
        var mapping = TgParameterMapping.of(div);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql, mapping)) {
            var parameter = TgParameterList.of(div.bind(divValue));
            if (success) {
                int count = ps.executeAndGetCount(tm, parameter);
                assertEquals(-1, count); // TODO 1

                var actual = selectValue();
                var expected = BigDecimal.ONE.divide(BigDecimal.valueOf(divValue), 2, RoundingMode.DOWN);
                assertEquals(expected, actual);
            } else {
                var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                    ps.executeAndGetCount(tm, parameter);
                });
                assertEqualsCode(SqlServiceCode.ERR_EXPRESSION_EVALUATION_FAILURE, e);
            }
        }
    }

    private void insert(BigDecimal value) throws IOException {
        var variable = TgVariable.ofDecimal(VNAME);
        var mapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(SQL, mapping)) {
            var parameter = TgParameterList.of(variable.bind(value));
            int count = ps.executeAndGetCount(tm, parameter);
            assertEquals(-1, count); // TODO 1
        }
    }

    private BigDecimal selectValue() throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery("select value from " + TEST)) {
            var entity = ps.executeAndFindRecord(tm).get();
            return entity.getDecimal(VNAME);
        }
    }
}
