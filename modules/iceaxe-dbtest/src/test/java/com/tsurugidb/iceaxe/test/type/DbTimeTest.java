package com.tsurugidb.iceaxe.test.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

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
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
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
                + "  value time" //
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
                    var parameter = TgBindParameters.of().addInt("pk", i).addTime("value", value(size, i));
                    transaction.executeAndGetCount(ps, parameter);
                }
                return;
            });
        }
    }

    @SuppressWarnings("unused")
    private static void insertLiteral(int size) throws IOException, InterruptedException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.execute(transaction -> {
            for (int i = 0; i < size; i++) {
                var insertSql = "insert into " + TEST + " values(" + i + ", time'" + value(size, i) + "')";
                try (var ps = session.createStatement(insertSql)) {
                    transaction.executeAndGetCount(ps);
                }
            }
            return;
        });
    }

    private static LocalTime value(int size, int i) {
        return LocalTime.of(23, 59, 1, (size - i) * 1000_000);
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

    @ParameterizedTest
    @ValueSource(strings = { "23:45:56.123456789", "00:00:00", "00:00:01", "00:00:00.000000001", "23:59:59.999999999" })
    void valueLiteral(String s) throws Exception {
        var expected = LocalTime.parse(s);

        var updateSql = "update " + TEST + " set value= time'" + s + "' where pk=1";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        int count = tm.executeAndGetCount(updateSql);
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
    void whereEq() throws Exception {
        var session = getSession();
        String sql = "select * from " + TEST + " where value = time'23:59:01.002'";
        var tm = createTransactionManagerOcc(session);
        TsurugiResultEntity entity = tm.executeAndFindRecord(sql).get();
        assertEquals(LocalTime.of(23, 59, 1, 2 * 1000_000), entity.getTime("value"));
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
        assertContains("compile failed with error:function_not_found message:\"set function not found: sum(time_of_day(without_time_zone))\" location:<input>:", e.getMessage());
    }

    @Test
    void localTime() throws Exception {
        var sql = "select localtime, localtimestamp from " + TEST;
        var tm = createTransactionManagerOcc(getSession());

        var list = tm.executeAndGetList(sql);

        assertEquals(SIZE, list.size());
        for (var actual : list) {
            LocalTime localTime = actual.getTime(0);
            LocalDateTime localTimestamp = actual.getDateTime(1);
            assertEquals(localTimestamp.toLocalTime(), localTime);
        }
    }

    @Test
    void kvsGet() throws Exception {
        var session = getSession();
        try (var client = KvsClient.attach(session.getLowSession()); // TODO Iceaxe KVS
                var txHandle = client.beginTransaction().await(10, TimeUnit.SECONDS)) {
            var key = new RecordBuffer().add("pk", 1);
            var result = client.get(txHandle, TEST, key).await(10, TimeUnit.SECONDS);
            assertEquals(1, result.size());

            client.commit(txHandle).await(10, TimeUnit.SECONDS);

            var record = result.asRecord();
            assertEquals(1, record.getInt("pk"));
            assertEquals(value(SIZE, 1), record.getTimeOfDay("value"));
        }
    }

    @Test
    void kvsPut() throws Exception {
        var expected = LocalTime.of(12, 34, 56, 789_000_001);

        var session = getSession();
        try (var client = KvsClient.attach(session.getLowSession()); // TODO Iceaxe KVS
                var txHandle = client.beginTransaction().await(10, TimeUnit.SECONDS)) {
            var record = new RecordBuffer().add("pk", 1).add("value", expected);
            var result = client.put(txHandle, TEST, record).await(10, TimeUnit.SECONDS);
            assertEquals(1, result.size());

            client.commit(txHandle).await(10, TimeUnit.SECONDS);
        }

        String sql = "select value from " + TEST + " where pk=1";
        var resultMapping = TgResultMapping.ofSingle(LocalTime.class);
        var tm = createTransactionManagerOcc(session);
        var result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(expected, result);
    }
}
