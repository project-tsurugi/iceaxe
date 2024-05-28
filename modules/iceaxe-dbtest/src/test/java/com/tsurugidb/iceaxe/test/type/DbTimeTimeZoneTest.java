package com.tsurugidb.iceaxe.test.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
 * time with time zone test
 */
class DbTimeTimeZoneTest extends DbTestTableTester {

    private static final int SIZE = 20;
    private static List<OffsetTime> LIST = IntStream.range(0, SIZE).mapToObj(i -> {
        int m = i / 5;
        int z = (i % 5) - 2;
        return OffsetTime.of(23, 59, 1, (SIZE / 5 - m) * 1000_000, ZoneOffset.ofHours(z));
    }).collect(Collectors.toList());

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
                + "value time with time zone" //
                + ")";
        var session = getSession();
        executeDdl(session, sql);
    }

    private static void insert(int size) throws IOException, InterruptedException {
        var session = getSession();
        var insertSql = "insert into " + TEST + " values(:pk, :value)";
        var insertMapping = TgParameterMapping.of(TgBindVariables.of().addInt("pk").addOffsetTime("value"));
        try (var ps = session.createStatement(insertSql, insertMapping)) {
            var tm = createTransactionManagerOcc(session);
            tm.execute(transaction -> {
                for (int i = 0; i < size; i++) {
                    var parameter = TgBindParameters.of().addInt("pk", i).addOffsetTime("value", value(size, i));
                    transaction.executeAndGetCount(ps, parameter);
                }
                return;
            });
        }
    }

    private static OffsetTime value(int size, int i) {
        return LIST.get(i);
    }

    @Test
    void tableMetadata() throws Exception {
        var session = getSession();
        var metadata = session.findTableMetadata(TEST).get();
        var list = metadata.getLowColumnList();
        assertEquals(2, list.size());
        assertColumn("pk", TgDataType.INT, list.get(0));
        assertColumn("value", TgDataType.OFFSET_TIME, list.get(1));
    }

    private static void assertColumn(String name, TgDataType type, SqlCommon.Column actual) {
        assertEquals(name, actual.getName());
        assertEquals(type.getLowDataType(), actual.getAtomType());
    }

    @ParameterizedTest
    @ValueSource(strings = { "23:45:56.123456789+09:00", "00:00:00Z", "00:00:01Z", "00:00:00.000000001-18:00", "23:59:59.999999999+18:00" })
    void value(String s) throws Exception {
        var expected = OffsetTime.parse(s);

        var variable = TgBindVariable.ofOffsetTime("value");
        var updateSql = "update " + TEST + " set value=" + variable + " where pk=1";
        var updateMapping = TgParameterMapping.of(variable);
        var updateParameter = TgBindParameters.of(variable.bind(expected));

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        int count = tm.executeAndGetCount(updateSql, updateMapping, updateParameter);
        assertEquals(1, count);

        var actual = tm.executeAndFindRecord("select * from " + TEST + " where pk=1").get();
        assertEquals(expected.withOffsetSameLocal(ZoneOffset.UTC), // TODO remove withOffsetSameLocal()
                actual.getOffsetTime("value"));
    }

    @Test
    void bindWhereEq() throws Exception {
        var variable = TgBindVariable.ofOffsetTime("value");
        var sql = "select * from " + TEST + " where value=" + variable;
        var mapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, mapping)) {
            for (var date : LIST) {
                var parameter = TgBindParameters.of(variable.bind(date));
                var list = tm.executeAndGetList(ps, parameter);
//TODO          assertEquals(1, list.size());
                for (var entity : list) {
                    assertEquals(toZ(date), entity.getOffsetTime("value")); // TODO remove toZ()
                }
            }
        }
    }

    // TODO remove toZ()
    private static OffsetTime toZ(OffsetTime date) {
        return date.withOffsetSameLocal(ZoneOffset.UTC);
    }

    @Test
    void bindWhereRange() throws Exception {
        var start = TgBindVariable.ofOffsetTime("start");
        var end = TgBindVariable.ofOffsetTime("end");
        var sql = "select * from " + TEST + " where " + start + "<=value and value<=" + end + " order by value";
        var mapping = TgParameterMapping.of(start, end);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, mapping)) {
            var start0 = OffsetTime.of(23, 59, 1, 2 * 1000_000, ZoneOffset.ofHours(1));
            var end0 = OffsetTime.of(23, 59, 1, 3 * 1000_000, ZoneOffset.ofHours(-1));
            var parameter = TgBindParameters.of(start.bind(start0), end.bind(end0));
            var list = tm.executeAndGetList(ps, parameter);

            var expectedList = LIST.stream().map(d -> toZ(d)) // TODO remove toZ()
                    .filter(d -> toZ(start0).compareTo(d) <= 0 && d.compareTo(toZ(end0)) <= 0) //
                    .sorted().collect(Collectors.toList());
            assertEquals(expectedList.size(), list.size());
            assertEquals(expectedList, list.stream().map(entity -> entity.getOffsetTime("value")).collect(Collectors.toList()));
        }
    }

    @Test
    @Disabled // TODO implicit conversion: char to timestamp with time zone
    void implicitConversion() throws Exception {
        var session = getSession();
        String sql = "select * from " + TEST + " where value = '2024-05-22 23:59:01.002+01:00'";
        var tm = createTransactionManagerOcc(session);
        TsurugiResultEntity entity = tm.executeAndFindRecord(sql).get();
        assertEquals(OffsetTime.of(23, 59, 1, 2 * 1000_000, ZoneOffset.ofHours(1)), entity.getOffsetTime("value"));
    }

    @Test
    @Disabled // TODO cast as timestamp with time zone
    void cast() throws Exception {
        var session = getSession();
        String sql = "update " + TEST + " set value = cast('2024-05-10 01:02:03.456+01:00' as timestamp with time zone)";
        var tm = createTransactionManagerOcc(session);
        int count = tm.executeAndGetCount(sql);
        assertEquals(SIZE, count);

        var list = tm.executeAndGetList("select * from " + TEST);
        assertEquals(count, list.size());
        var expected = OffsetTime.parse("2024-05-10T01:02:03.456+01:00");
        for (var entity : list) {
            assertEquals(expected, entity.getOffsetTime("value"));
        }
    }

    @Test
    void min() throws Exception {
        var session = getSession();
        String sql = "select min(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(LocalTime.class); // TODO OffsetTime
        var tm = createTransactionManagerOcc(session);
        LocalTime result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(LIST.stream().min(OffsetTime::compareTo).get().toLocalTime(), result);
    }

    @Test
    void max() throws Exception {
        var session = getSession();
        String sql = "select max(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(LocalTime.class); // TODO OffsetTime
        var tm = createTransactionManagerOcc(session);
        LocalTime result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(LIST.stream().max(OffsetTime::compareTo).get().toLocalTime(), result);
    }

    @Test
    void sum() throws Exception {
        var session = getSession();
        String sql = "select sum(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(OffsetTime.class);
        var tm = createTransactionManagerOcc(session);
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            tm.executeAndFindRecord(sql, resultMapping);
        });
        assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e);
        assertContains("function 'sum' is not found", e.getMessage());
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
            assertEquals(value(SIZE, 1).toLocalTime(), record.getTimeOfDay("value"));// TODO assertEquals(value(SIZE, 1), record.getTimeOfDayWithTimeZone("value"));
        }
    }

    @Test
    @Disabled // TODO remove Disabled. KVS put for time with time zone
    void kvsPut() throws Exception {
        var expected = OffsetTime.of(12, 34, 56, 789_000_001, ZoneOffset.ofHours(9));

        var session = getSession();
        try (var client = KvsClient.attach(session.getLowSession()); // TODO Iceaxe KVS
                var txHandle = client.beginTransaction().await(10, TimeUnit.SECONDS)) {
            var record = new RecordBuffer().add("pk", 1).add("value", expected);
            var result = client.put(txHandle, TEST, record).await(10, TimeUnit.SECONDS);
            assertEquals(1, result.size());

            client.commit(txHandle).await(10, TimeUnit.SECONDS);
        }

        String sql = "select value from " + TEST + " where pk=1";
        var resultMapping = TgResultMapping.ofSingle(OffsetTime.class);
        var tm = createTransactionManagerOcc(session);
        var result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(expected, result);
    }
}
