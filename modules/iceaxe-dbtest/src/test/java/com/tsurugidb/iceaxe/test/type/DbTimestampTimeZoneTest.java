package com.tsurugidb.iceaxe.test.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
 * timestamp with time zone test
 */
class DbTimestampTimeZoneTest extends DbTestTableTester {

    private static final int SIZE = 20;
    private static List<OffsetDateTime> LIST = IntStream.range(0, SIZE).mapToObj(i -> {
        int m = i / 5;
        int z = (i % 5) - 2;
        return OffsetDateTime.of(2024, 5, 22, 23, 59, 1, (SIZE / 5 - m) * 1000_000, ZoneOffset.ofHours(z));
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
                + "value timestamp with time zone" //
                + ")";
        var session = getSession();
        executeDdl(session, sql);
    }

    private static void insert(int size) throws IOException, InterruptedException {
        var session = getSession();
        var insertSql = "insert into " + TEST + " values(:pk, :value)";
        var insertMapping = TgParameterMapping.of(TgBindVariables.of().addInt("pk").addOffsetDateTime("value"));
        try (var ps = session.createStatement(insertSql, insertMapping)) {
            var tm = createTransactionManagerOcc(session);
            tm.execute(transaction -> {
                for (int i = 0; i < size; i++) {
                    var parameter = TgBindParameters.of().addInt("pk", i).addOffsetDateTime("value", LIST.get(i));
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
        assertColumn("pk", TgDataType.INT, list.get(0));
        assertColumn("value", TgDataType.OFFSET_DATE_TIME, list.get(1));
    }

    private static void assertColumn(String name, TgDataType type, SqlCommon.Column actual) {
        assertEquals(name, actual.getName());
        assertEquals(type.getLowDataType(), actual.getAtomType());
    }

    @Test
    void bindWhereEq() throws Exception {
        var variable = TgBindVariable.ofOffsetDateTime("value");
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
                    assertEquals(toZ(date), entity.getOffsetDateTime("value")); // TODO remove toZ()
                }
            }
        }
    }

    // TODO remove toZ()
    private static OffsetDateTime toZ(OffsetDateTime date) {
        return date.atZoneSimilarLocal(ZoneId.of("UTC")).toOffsetDateTime();
    }

    @Test
    void bindWhereRange() throws Exception {
        var start = TgBindVariable.ofOffsetDateTime("start");
        var end = TgBindVariable.ofOffsetDateTime("end");
        var sql = "select * from " + TEST + " where " + start + "<=value and value<=" + end + " order by value";
        var mapping = TgParameterMapping.of(start, end);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, mapping)) {
            var start0 = OffsetDateTime.of(2024, 5, 22, 23, 59, 1, 2 * 1000_000, ZoneOffset.ofHours(1));
            var end0 = OffsetDateTime.of(2024, 5, 22, 23, 59, 1, 3 * 1000_000, ZoneOffset.ofHours(-1));
            var parameter = TgBindParameters.of(start.bind(start0), end.bind(end0));
            var list = tm.executeAndGetList(ps, parameter);

            var expectedList = LIST.stream().map(d -> toZ(d)) // TODO remove toZ()
                    .filter(d -> toZ(start0).compareTo(d) <= 0 && d.compareTo(toZ(end0)) <= 0) //
                    .sorted().collect(Collectors.toList());
            assertEquals(expectedList.size(), list.size());
            assertEquals(expectedList, list.stream().map(entity -> entity.getOffsetDateTime("value")).collect(Collectors.toList()));
        }
    }

    @Test
    @Disabled // TODO implicit conversion: char to timestamp with time zone
    void implicitConversion() throws Exception {
        var session = getSession();
        String sql = "select * from " + TEST + " where value = '2024-05-22 23:59:01.002+01:00'";
        var tm = createTransactionManagerOcc(session);
        TsurugiResultEntity entity = tm.executeAndFindRecord(sql).get();
        assertEquals(OffsetDateTime.of(2024, 5, 22, 23, 59, 1, 2 * 1000_000, ZoneOffset.ofHours(1)), entity.getOffsetDateTime("value"));
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
        var expected = OffsetDateTime.parse("2024-05-10T01:02:03.456+01:00");
        for (var entity : list) {
            assertEquals(expected, entity.getOffsetDateTime("value"));
        }
    }

    @Test
    void min() throws Exception {
        var session = getSession();
        String sql = "select min(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(LocalDateTime.class); // TODO OffsetDateTime
        var tm = createTransactionManagerOcc(session);
        LocalDateTime result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(LIST.stream().min(OffsetDateTime::compareTo).get().toLocalDateTime(), result);
    }

    @Test
    void max() throws Exception {
        var session = getSession();
        String sql = "select max(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(LocalDateTime.class); // TODO OffsetDateTime
        var tm = createTransactionManagerOcc(session);
        LocalDateTime result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(LIST.stream().max(OffsetDateTime::compareTo).get().toLocalDateTime(), result);
    }

    @Test
    void sum() throws Exception {
        var session = getSession();
        String sql = "select sum(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(OffsetDateTime.class);
        var tm = createTransactionManagerOcc(session);
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            tm.executeAndFindRecord(sql, resultMapping);
        });
        assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e);
        assertContains("function 'sum' is not found", e.getMessage());
    }
}
