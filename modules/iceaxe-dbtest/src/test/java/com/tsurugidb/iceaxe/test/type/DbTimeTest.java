package com.tsurugidb.iceaxe.test.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.time.LocalTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * time test
 */
class DbTimeTest extends DbTestTableTester {

    private static final int SIZE = 5;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTable();
        insert(SIZE);

        logInitEnd(info);
    }

    private static void createTable() throws IOException, InterruptedException {
        String sql = "create table " + TEST + "(" //
                + "  pk int primary key," //
                + "value time" //
                + ")";
        var session = getSession();
        executeDdl(session, sql);
    }

    private static void insert(int size) throws IOException, InterruptedException {
        var session = getSession();
        var insertSql = "insert into " + TEST + " values(:pk, :value)";
        var insertMapping = TgParameterMapping.of(TgBindVariables.of().addInt("pk").addTime("value"));
        try (var ps = session.createStatement(insertSql, insertMapping)) {
            var tm = createTransactionManagerOcc(session);
            tm.execute(transaction -> {
                for (int i = 0; i < size; i++) {
                    var parameter = TgBindParameters.of().addInt("pk", i).addTime("value", LocalTime.of(23, 59, 1, (size - i) * 1000_000));
                    transaction.executeAndGetCount(ps, parameter);
                }
                return;
            });
        }
    }

    @Test
    void tableMetadata() throws Exception {
        var session = getSession();
        var metadata = session.findTableMetadata(TEST).get();
        var list = metadata.getLowColumnList();
        assertEquals(2, list.size());
        assertColumn("pk", TgDataType.INT, list.get(0));
        assertColumn("value", TgDataType.TIME, list.get(1));
    }

    private static void assertColumn(String name, TgDataType type, SqlCommon.Column actual) {
        assertEquals(name, actual.getName());
        assertEquals(type.getLowDataType(), actual.getAtomType());
    }

    @ParameterizedTest
    @ValueSource(strings = { "23:45:56.123456789", "00:00:00", "00:00:01", "00:00:00.000000001", "23:59:59.999999999" })
    void value(String s) throws Exception {
        var expected = LocalTime.parse(s);

        var variable = TgBindVariable.ofTime("value");
        var updateSql = "update " + TEST + " set value=" + variable + " where pk=1";
        var updateMapping = TgParameterMapping.of(variable);
        var updateParameter = TgBindParameters.of(variable.bind(expected));

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        int count = tm.executeAndGetCount(updateSql, updateMapping, updateParameter);
        assertEquals(1, count);

        var actual = tm.executeAndFindRecord("select * from " + TEST + " where pk=1").get();
        assertEquals(expected, actual.getTime("value"));
    }

    @Test
    void bindWhereEq() throws Exception {
        var variable = TgBindVariable.ofTime("value");
        var sql = "select * from " + TEST + " where value=" + variable;
        var mapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, mapping)) {
            var time = LocalTime.of(23, 59, 1, 2 * 1000_000);
            var parameter = TgBindParameters.of(variable.bind(time));
            var entity = tm.executeAndFindRecord(ps, parameter).get();
            assertEquals(time, entity.getTime("value"));
        }
    }

    @Test
    void bindWhereRange() throws Exception {
        var start = TgBindVariable.ofTime("start");
        var end = TgBindVariable.ofTime("end");
        var sql = "select * from " + TEST + " where " + start + "<=value and value<=" + end + " order by value";
        var mapping = TgParameterMapping.of(start, end);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, mapping)) {
            var parameter = TgBindParameters.of(start.bind(LocalTime.of(23, 59, 1, 2 * 1000_000)), end.bind(LocalTime.of(23, 59, 1, 3 * 1000_000)));
            var list = tm.executeAndGetList(ps, parameter);
            assertEquals(2, list.size());
            assertEquals(LocalTime.of(23, 59, 1, 2 * 1000_000), list.get(0).getTime("value"));
            assertEquals(LocalTime.of(23, 59, 1, 3 * 1000_000), list.get(1).getTime("value"));
        }
    }

    @Test
    @Disabled // TODO implicit conversion: char to time
    void implicitConversion() throws Exception {
        var session = getSession();
        String sql = "select * from " + TEST + " where value = '23:59:01.002'";
        var tm = createTransactionManagerOcc(session);
        TsurugiResultEntity entity = tm.executeAndFindRecord(sql).get();
        assertEquals(LocalTime.of(23, 59, 1, 2 * 1000_000), entity.getTime("value"));
    }

    @Test
    @Disabled // TODO cast as time
    void cast() throws Exception {
        var session = getSession();
        String sql = "update " + TEST + " set value = cast('01:02:03.456' as time)";
        var tm = createTransactionManagerOcc(session);
        int count = tm.executeAndGetCount(sql);
        assertEquals(SIZE, count);
    }

    @Test
    void min() throws Exception {
        var session = getSession();
        String sql = "select min(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(LocalTime.class);
        var tm = createTransactionManagerOcc(session);
        LocalTime result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(LocalTime.of(23, 59, 1, 1 * 1000_000), result);
    }

    @Test
    void max() throws Exception {
        var session = getSession();
        String sql = "select max(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(LocalTime.class);
        var tm = createTransactionManagerOcc(session);
        LocalTime result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(LocalTime.of(23, 59, 1, SIZE * 1000_000), result);
    }

    @Test
    void sum() throws Exception {
        var session = getSession();
        String sql = "select sum(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(LocalTime.class);
        var tm = createTransactionManagerOcc(session);
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            tm.executeAndFindRecord(sql, resultMapping);
        });
        assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e);
        assertContains("function 'sum' is not found", e.getMessage());
    }
}
