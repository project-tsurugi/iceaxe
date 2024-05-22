package com.tsurugidb.iceaxe.test.type;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

/**
 * decimal(5,0) test
 */
class DbDecimal0Test extends DbTestTableTester {

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
                + "  value decimal(5,0)" //
                + ")";
        var session = getSession();
        executeDdl(session, sql);
    }

    private static void insert(int size) throws IOException, InterruptedException {
        var session = getSession();
        var insertSql = "insert into " + TEST + " values(:pk, :value)";
        var insertMapping = TgParameterMapping.of(TgBindVariables.of().addInt("pk").addDecimal("value"));
        try (var ps = session.createStatement(insertSql, insertMapping)) {
            var tm = createTransactionManagerOcc(session);
            tm.execute(transaction -> {
                for (int i = 0; i < size; i++) {
                    var parameter = TgBindParameters.of().addInt("pk", i).addDecimal("value", BigDecimal.valueOf(size - i - 1));
                    transaction.executeAndGetCount(ps, parameter);
                }
                return;
            });
        }
    }

    @Test
    void tableMetadate() throws Exception {
        var session = getSession();
        var metadata = session.findTableMetadata(TEST).get();
        var list = metadata.getLowColumnList();
        assertEquals(2, list.size());
        assertColumn("pk", AtomType.INT4, list.get(0));
        assertColumn("value", AtomType.DECIMAL, list.get(1));
    }

    private static void assertColumn(String name, AtomType type, SqlCommon.Column actual) {
        assertEquals(name, actual.getName());
        assertEquals(type, actual.getAtomType());
    }

    @Test
    void bindWhereEq() throws Exception {
        var variable = TgBindVariable.ofDecimal("value");
        var sql = "select * from " + TEST + " where value=" + variable;
        var mapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, mapping)) {
            var value = BigDecimal.valueOf(2);
            var parameter = TgBindParameters.of(variable.bind(value));
            var entity = tm.executeAndFindRecord(ps, parameter).get();
            assertEquals(value, entity.getDecimal("value"));
        }
    }

    @Test
    void bindWhereRange() throws Exception {
        var start = TgBindVariable.ofDecimal("start");
        var end = TgBindVariable.ofDecimal("end");
        var sql = "select * from " + TEST + " where " + start + "<=value and value<=" + end + " order by value";
        var mapping = TgParameterMapping.of(start, end);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, mapping)) {
            var parameter = TgBindParameters.of(start.bind(BigDecimal.valueOf(2)), end.bind(BigDecimal.valueOf(3)));
            var list = tm.executeAndGetList(ps, parameter);
            assertEquals(2, list.size());
            assertEquals(BigDecimal.valueOf(2), list.get(0).getDecimal("value"));
            assertEquals(BigDecimal.valueOf(3), list.get(1).getDecimal("value"));
        }
    }

    @Test
    void whereEq() throws Exception {
        var session = getSession();
        String sql = "select * from " + TEST + " where value = 2";
        var tm = createTransactionManagerOcc(session);
        TsurugiResultEntity entity = tm.executeAndFindRecord(sql).get();
        assertEquals(BigDecimal.valueOf(2), entity.getDecimal("value"));
    }

    @Test
    @Disabled // TODO implicit conversion: char to decimal
    void implicitConversion() throws Exception {
        var session = getSession();
        String sql = "select * from " + TEST + " where value = '2'";
        var tm = createTransactionManagerOcc(session);
        TsurugiResultEntity entity = tm.executeAndFindRecord(sql).get();
        assertEquals(BigDecimal.valueOf(2), entity.getDecimal("value"));
    }

    @Test
    void cast() throws Exception {
        var session = getSession();
        String sql = "update " + TEST + " set value = cast('7' as decimal(5,0))";
        var tm = createTransactionManagerOcc(session);
        int count = tm.executeAndGetCount(sql);
        assertEquals(SIZE, count);

        var list = tm.executeAndGetList("select * from " + TEST);
        assertEquals(SIZE, list.size());
        for (var actual : list) {
            assertEquals(BigDecimal.valueOf(7), actual.getDecimal("value"));
        }
    }

    @Test
    @Disabled // TODO min(decimal(5,0))
    void min() throws Exception {
        var session = getSession();
        String sql = "select min(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(BigDecimal.class);
        var tm = createTransactionManagerOcc(session);
        BigDecimal result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(BigDecimal.valueOf(0), result);
    }

    @Test
    @Disabled // TODO max(decimal(5,0))
    void max() throws Exception {
        var session = getSession();
        String sql = "select max(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(BigDecimal.class);
        var tm = createTransactionManagerOcc(session);
        BigDecimal result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(BigDecimal.valueOf(SIZE - 1), result);
    }

    @Test
    @Disabled // TODO sum(decimal(5,0))
    void sum() throws Exception {
        var session = getSession();
        String sql = "select sum(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(BigDecimal.class);
        var tm = createTransactionManagerOcc(session);
        BigDecimal result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(BigDecimal.valueOf(IntStream.range(0, SIZE).sum()), result);
    }

    @Test
    @Disabled // TODO avg(decimal(5,0))
    void avg() throws Exception {
        var session = getSession();
        String sql = "select avg(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(BigDecimal.class);
        var tm = createTransactionManagerOcc(session);
        BigDecimal result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(BigDecimal.valueOf(IntStream.range(0, SIZE).sum() / SIZE), result);
    }

    @Test
    void minCastAny() throws Exception {
        var session = getSession();
        String sql = "select min(cast(value as decimal(*,*))) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(BigDecimal.class);
        var tm = createTransactionManagerOcc(session);
        BigDecimal result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(BigDecimal.valueOf(0), result);
    }

    @Test
    void maxCastAny() throws Exception {
        var session = getSession();
        String sql = "select max(cast(value as decimal(*,*))) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(BigDecimal.class);
        var tm = createTransactionManagerOcc(session);
        BigDecimal result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(BigDecimal.valueOf(SIZE - 1), result);
    }

    @Test
    void sumCastAny() throws Exception {
        var session = getSession();
        String sql = "select sum(cast(value as decimal(*,*))) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(BigDecimal.class);
        var tm = createTransactionManagerOcc(session);
        BigDecimal result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(BigDecimal.valueOf(IntStream.range(0, SIZE).sum()), result);
    }

    @Test
    void avgCastAny() throws Exception {
        var session = getSession();
        String sql = "select avg(cast(value as decimal(*,*))) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(BigDecimal.class);
        var tm = createTransactionManagerOcc(session);
        BigDecimal result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(BigDecimal.valueOf(IntStream.range(0, SIZE).sum() / SIZE), result);
    }
}
