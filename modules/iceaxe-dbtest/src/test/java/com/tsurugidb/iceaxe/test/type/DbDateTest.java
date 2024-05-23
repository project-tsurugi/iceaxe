package com.tsurugidb.iceaxe.test.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.time.LocalDate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

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
 * date test
 */
class DbDateTest extends DbTestTableTester {

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
                + "value date" //
                + ")";
        var session = getSession();
        executeDdl(session, sql);
    }

    private static void insert(int size) throws IOException, InterruptedException {
        var session = getSession();
        var insertSql = "insert into " + TEST + " values(:pk, :value)";
        var insertMapping = TgParameterMapping.of(TgBindVariables.of().addInt("pk").addDate("value"));
        try (var ps = session.createStatement(insertSql, insertMapping)) {
            var tm = createTransactionManagerOcc(session);
            tm.execute(transaction -> {
                for (int i = 0; i < size; i++) {
                    var parameter = TgBindParameters.of().addInt("pk", i).addDate("value", LocalDate.of(2024, 5, size - i));
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
        assertColumn("value", TgDataType.DATE, list.get(1));
    }

    private static void assertColumn(String name, TgDataType type, SqlCommon.Column actual) {
        assertEquals(name, actual.getName());
        assertEquals(type.getLowDataType(), actual.getAtomType());
    }

    @Test
    void bindWhereEq() throws Exception {
        var variable = TgBindVariable.ofDate("value");
        var sql = "select * from " + TEST + " where value=" + variable;
        var mapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, mapping)) {
            var date = LocalDate.of(2024, 5, 2);
            var parameter = TgBindParameters.of(variable.bind(date));
            var entity = tm.executeAndFindRecord(ps, parameter).get();
            assertEquals(date, entity.getDate("value"));
        }
    }

    @Test
    void bindWhereRange() throws Exception {
        var start = TgBindVariable.ofDate("start");
        var end = TgBindVariable.ofDate("end");
        var sql = "select * from " + TEST + " where " + start + "<=value and value<=" + end + " order by value";
        var mapping = TgParameterMapping.of(start, end);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, mapping)) {
            var parameter = TgBindParameters.of(start.bind(LocalDate.of(2024, 5, 2)), end.bind(LocalDate.of(2024, 5, 3)));
            var list = tm.executeAndGetList(ps, parameter);
            assertEquals(2, list.size());
            assertEquals(LocalDate.of(2024, 5, 2), list.get(0).getDate("value"));
            assertEquals(LocalDate.of(2024, 5, 3), list.get(1).getDate("value"));
        }
    }

    @Test
    @Disabled // TODO implicit conversion: char to date
    void implicitConversion() throws Exception {
        var session = getSession();
        String sql = "select * from " + TEST + " where value = '2024-05-02'";
        var tm = createTransactionManagerOcc(session);
        TsurugiResultEntity entity = tm.executeAndFindRecord(sql).get();
        assertEquals(LocalDate.of(2024, 5, 2), entity.getDate("value"));
    }

    @Test
    @Disabled // TODO cast as date
    void cast() throws Exception {
        var session = getSession();
        String sql = "update " + TEST + " set value = cast('2024-05-07' as date)";
        var tm = createTransactionManagerOcc(session);
        int count = tm.executeAndGetCount(sql);
        assertEquals(SIZE, count);
    }

    @Test
    void min() throws Exception {
        var session = getSession();
        String sql = "select min(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(LocalDate.class);
        var tm = createTransactionManagerOcc(session);
        LocalDate result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(LocalDate.of(2024, 5, 1), result);
    }

    @Test
    void max() throws Exception {
        var session = getSession();
        String sql = "select max(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(LocalDate.class);
        var tm = createTransactionManagerOcc(session);
        LocalDate result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(LocalDate.of(2024, 5, SIZE), result);
    }

    @Test
    void sum() throws Exception {
        var session = getSession();
        String sql = "select sum(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(LocalDate.class);
        var tm = createTransactionManagerOcc(session);
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            tm.executeAndFindRecord(sql, resultMapping);
        });
        assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e);
        assertContains("function 'sum' is not found", e.getMessage());
    }
}
