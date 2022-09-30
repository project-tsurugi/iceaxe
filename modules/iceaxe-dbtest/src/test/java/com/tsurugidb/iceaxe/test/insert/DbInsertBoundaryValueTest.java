package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
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
        var variable = TgVariable.ofInt4("value");

        var insertSql = "insert into " + TEST //
                + "(" + name + ")" //
                + " values(" + variable + ")";
        var insertMapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(insertSql, insertMapping)) {
            var param = TgParameterList.of(variable.bind(value));
            ps.executeAndGetCount(tm, param);
        }

        var selectSql = "select " + name + " from " + TEST;
        var selectMapping = TgResultMapping.of(record -> record.nextInt4());
        try (var ps = session.createPreparedQuery(selectSql, selectMapping)) {
            var actual = ps.executeAndFindRecord(tm).get();
            assertEquals(value, actual);
        }
    }

    @ParameterizedTest
    @ValueSource(longs = { Long.MIN_VALUE, Long.MAX_VALUE })
    void insertInt8(long value) throws IOException {
        String name = "int8";
        var variable = TgVariable.ofInt8("value");

        var insertSql = "insert into " + TEST //
                + "(" + name + ")" //
                + " values(" + variable + ")";
        var insertMapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(insertSql, insertMapping)) {
            var param = TgParameterList.of(variable.bind(value));
            ps.executeAndGetCount(tm, param);
        }

        var selectSql = "select " + name + " from " + TEST;
        var selectMapping = TgResultMapping.of(record -> record.nextInt8());
        try (var ps = session.createPreparedQuery(selectSql, selectMapping)) {
            var actual = ps.executeAndFindRecord(tm).get();
            assertEquals(value, actual);
        }
    }

    @ParameterizedTest
    @ValueSource(floats = { Float.MIN_VALUE, Float.MAX_VALUE, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, Float.NaN })
    void insertFloat4(float value) throws IOException {
        String name = "float4";
        var variable = TgVariable.ofFloat4("value");

        var insertSql = "insert into " + TEST //
                + "(" + name + ")" //
                + " values(" + variable + ")";
        var insertMapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(insertSql, insertMapping)) {
            var param = TgParameterList.of(variable.bind(value));
            ps.executeAndGetCount(tm, param);
        }

        var selectSql = "select " + name + " from " + TEST;
        var selectMapping = TgResultMapping.of(record -> record.nextFloat4());
        try (var ps = session.createPreparedQuery(selectSql, selectMapping)) {
            var actual = ps.executeAndFindRecord(tm).get();
            assertEquals(value, actual);
        }
    }

    @ParameterizedTest
    @ValueSource(doubles = { Double.MIN_VALUE, Double.MAX_VALUE, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN })
    void insertFloat8(double value) throws IOException {
        String name = "float8";
        var variable = TgVariable.ofFloat8("value");

        var insertSql = "insert into " + TEST //
                + "(" + name + ")" //
                + " values(" + variable + ")";
        var insertMapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(insertSql, insertMapping)) {
            var param = TgParameterList.of(variable.bind(value));
            ps.executeAndGetCount(tm, param);
        }

        var selectSql = "select " + name + " from " + TEST;
        var selectMapping = TgResultMapping.of(record -> record.nextFloat8());
        try (var ps = session.createPreparedQuery(selectSql, selectMapping)) {
            var actual = ps.executeAndFindRecord(tm).get();
            assertEquals(value, actual);
        }
    }
}
