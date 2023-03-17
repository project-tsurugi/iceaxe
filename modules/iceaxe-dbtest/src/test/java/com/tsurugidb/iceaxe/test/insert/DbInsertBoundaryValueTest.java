package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * insert boundary value test
 */
class DbInsertBoundaryValueTest extends DbTestTableTester {

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
                + "  int4 int," //
                + "  int8 bigint," //
                + "  float4 float," //
                + "  float8 double" //
                + ")";
        executeDdl(getSession(), sql);
    }

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, Integer.MAX_VALUE })
    void insertInt4(int value) throws IOException {
        String name = "int4";
        var variable = TgBindVariable.ofInt("value");

        var insertSql = "insert into " + TEST //
                + "(" + name + ")" //
                + " values(" + variable + ")";
        var insertMapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(insertSql, insertMapping)) {
            var parameter = TgBindParameters.of(variable.bind(value));
            tm.executeAndGetCount(ps, parameter);
        }

        var selectSql = "select " + name + " from " + TEST;
        var selectMapping = TgResultMapping.of(record -> record.nextInt());
        try (var ps = session.createQuery(selectSql, selectMapping)) {
            var actual = tm.executeAndFindRecord(ps).get();
            assertEquals(value, actual);
        }
    }

    @ParameterizedTest
    @ValueSource(longs = { Long.MIN_VALUE, Long.MAX_VALUE })
    void insertInt8(long value) throws IOException {
        String name = "int8";
        var variable = TgBindVariable.ofLong("value");

        var insertSql = "insert into " + TEST //
                + "(" + name + ")" //
                + " values(" + variable + ")";
        var insertMapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(insertSql, insertMapping)) {
            var parameter = TgBindParameters.of(variable.bind(value));
            tm.executeAndGetCount(ps, parameter);
        }

        var selectSql = "select " + name + " from " + TEST;
        var selectMapping = TgResultMapping.of(record -> record.nextLong());
        try (var ps = session.createQuery(selectSql, selectMapping)) {
            var actual = tm.executeAndFindRecord(ps).get();
            assertEquals(value, actual);
        }
    }

    @ParameterizedTest
    @ValueSource(floats = { Float.MIN_VALUE, Float.MAX_VALUE, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, Float.NaN })
    void insertFloat4(float value) throws IOException {
        String name = "float4";
        var variable = TgBindVariable.ofFloat("value");

        var insertSql = "insert into " + TEST //
                + "(" + name + ")" //
                + " values(" + variable + ")";
        var insertMapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(insertSql, insertMapping)) {
            var parameter = TgBindParameters.of(variable.bind(value));
            tm.executeAndGetCount(ps, parameter);
        }

        var selectSql = "select " + name + " from " + TEST;
        var selectMapping = TgResultMapping.of(record -> record.nextFloat());
        try (var ps = session.createQuery(selectSql, selectMapping)) {
            var actual = tm.executeAndFindRecord(ps).get();
            assertEquals(value, actual);
        }
    }

    @ParameterizedTest
    @ValueSource(doubles = { Double.MIN_VALUE, Double.MAX_VALUE, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN })
    void insertFloat8(double value) throws IOException {
        String name = "float8";
        var variable = TgBindVariable.ofDouble("value");

        var insertSql = "insert into " + TEST //
                + "(" + name + ")" //
                + " values(" + variable + ")";
        var insertMapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(insertSql, insertMapping)) {
            var parameter = TgBindParameters.of(variable.bind(value));
            tm.executeAndGetCount(ps, parameter);
        }

        var selectSql = "select " + name + " from " + TEST;
        var selectMapping = TgResultMapping.of(record -> record.nextDouble());
        try (var ps = session.createQuery(selectSql, selectMapping)) {
            var actual = tm.executeAndFindRecord(ps).get();
            assertEquals(value, actual);
        }
    }
}
