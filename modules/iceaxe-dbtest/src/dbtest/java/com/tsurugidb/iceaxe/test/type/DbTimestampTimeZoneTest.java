/*
 * Copyright 2023-2026 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.iceaxe.test.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.OffsetDateTime;
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

import com.tsurugidb.iceaxe.metadata.TgSqlColumn;
import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * timestamp with time zone test
 */
class DbTimestampTimeZoneTest extends DbTestTableTester {

    private static final boolean CHECK_CURRENT_TIMESTAMP = false;

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
                + "  value timestamp with time zone" //
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
                    var parameter = TgBindParameters.of().addInt("pk", i).addOffsetDateTime("value", value(i));
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
                var insertSql = "insert into " + TEST + " values(" + i + ", timestamp with time zone'" + value(i) + "')";
                try (var ps = session.createStatement(insertSql)) {
                    transaction.executeAndGetCount(ps);
                }
            }
            return;
        });
    }

    private static OffsetDateTime value(int i) {
        return LIST.get(i);
    }

    @Test
    void tableMetadata() throws Exception {
        var session = getSession();
        var metadata = session.findTableMetadata(TEST).get();
        var list = metadata.getColumnList();
        assertEquals(2, list.size());
        assertColumn("pk", TgDataType.INT, "INT", list.get(0));
        assertColumn("value", TgDataType.OFFSET_DATE_TIME, "TIMESTAMP WITH TIME ZONE", list.get(1));
    }

    private static void assertColumn(String name, TgDataType type, String sqlType, TgSqlColumn actual) {
        assertEquals(name, actual.getName());
        assertEquals(type, actual.getDataType());
        assertEquals(sqlType, actual.getSqlType());
    }

    @ParameterizedTest
    @ValueSource(strings = { "2024-05-24T23:45:56.123456789+09:00", "0000-01-01T00:00:00Z", "0001-01-01T00:00:00Z", "0001-01-01T00:00:01Z", "0001-01-01T00:00:00.000000001Z", "1970-01-01T00:00:00Z",
            "1969-12-31T00:00:01Z", "9999-12-31T23:59:59.999999999Z", "-999999999-01-01T00:00:00-18:00", "+99999999-12-31T23:59:59.999999999+18:00" })
    void value(String s) throws Exception {
        var expected = OffsetDateTime.parse(s);

        var variable = TgBindVariable.ofOffsetDateTime("value");
        var updateSql = "update " + TEST + " set value=" + variable + " where pk=1";
        var updateMapping = TgParameterMapping.of(variable);
        var updateParameter = TgBindParameters.of(variable.bind(expected));

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        int count = tm.executeAndGetCount(updateSql, updateMapping, updateParameter);
        assertEquals(1, count);

        var actual = tm.executeAndFindRecord("select * from " + TEST + " where pk=1").get();
        assertEquals(toZ(expected), toZ(actual.getOffsetDateTime("value")));
    }

    @ParameterizedTest
    @ValueSource(strings = { "2024-05-24T23:45:56.123456789+09:00", "0000-01-01T00:00:00Z", "0001-01-01T00:00:00Z", "0001-01-01T00:00:01Z", "0001-01-01T00:00:00.000000001Z", "1970-01-01T00:00:00Z",
            "1969-12-31T00:00:01Z", "9999-12-31T23:59:59.999999999Z", "-999999999-01-01T00:00:00-18:00", "+99999999-12-31T23:59:59.999999999+18:00" })
    void valueLiteral(String s) throws Exception {
        var expected = OffsetDateTime.parse(s);
        if (s.startsWith("+")) {
            s = s.substring(1);
        }

        var updateSql = "update " + TEST + " set value= timestamp with time zone'" + s + "' where pk=1";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);

        if (expected.getYear() <= 0) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                tm.executeAndGetCount(updateSql);
            });
            assertEqualsCode(SqlServiceCode.VALUE_ANALYZE_EXCEPTION, e);
            return;
        }

        int count = tm.executeAndGetCount(updateSql);
        assertEquals(1, count);

        var actual = tm.executeAndFindRecord("select * from " + TEST + " where pk=1").get();
        assertEquals(toZ(expected), toZ(actual.getOffsetDateTime("value")));
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
                assertEquals(1, list.size());
                for (var entity : list) {
                    assertEquals(toZ(date), toZ(entity.getOffsetDateTime("value")));
                }
            }
        }
    }

    private static OffsetDateTime toZ(OffsetDateTime date) {
        return date.withOffsetSameInstant(ZoneOffset.UTC);
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

            var expectedList = LIST.stream().map(v -> toZ(v)) //
                    .filter(d -> toZ(start0).compareTo(d) <= 0 && d.compareTo(toZ(end0)) <= 0) //
                    .sorted().collect(Collectors.toList());
            assertEquals(expectedList.size(), list.size());
            assertEquals(expectedList, list.stream().map(entity -> toZ(entity.getOffsetDateTime("value"))).collect(Collectors.toList()));
        }
    }

    @Test
    void whereEq() throws Exception {
        var session = getSession();
        String sql = "select * from " + TEST + " where value = timestamp with time zone'2024-05-22 23:59:01.002+01:00'";
        var tm = createTransactionManagerOcc(session);
        TsurugiResultEntity entity = tm.executeAndFindRecord(sql).get();
        assertEquals(toZ(OffsetDateTime.of(2024, 5, 22, 23, 59, 1, 2 * 1000_000, ZoneOffset.ofHours(1))), toZ(entity.getOffsetDateTime("value")));
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
        var resultMapping = TgResultMapping.ofSingle(OffsetDateTime.class);
        var tm = createTransactionManagerOcc(session);
        OffsetDateTime result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(LIST.stream().map(v -> toZ(v)).min(OffsetDateTime::compareTo).get(), toZ(result));
    }

    @Test
    void max() throws Exception {
        var session = getSession();
        String sql = "select max(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(OffsetDateTime.class);
        var tm = createTransactionManagerOcc(session);
        OffsetDateTime result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(LIST.stream().map(v -> toZ(v)).max(OffsetDateTime::compareTo).get(), toZ(result));
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
        assertContains("compile failed with error:function_not_found message:\"set function not found: sum(time_point(with_time_zone))\" location:<input>:", e.getMessage());
    }

    @Test
    void currentTimestamp() throws Exception {
        var sql = "select current_timestamp from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(OffsetDateTime.class);
        var tm = createTransactionManagerOcc(getSession());

        var nowBeforeTransaction = toZ(OffsetDateTime.now());
        var list = tm.executeAndGetList(sql, resultMapping);
        var nowAfterTransaction = toZ(OffsetDateTime.now());

        assertEquals(SIZE, list.size());
        OffsetDateTime first = null;
        for (OffsetDateTime currentTimestamp : list) {
            if (first == null) {
                first = currentTimestamp;
            } else {
                assertEquals(first, currentTimestamp);
            }

            if (CHECK_CURRENT_TIMESTAMP) {
                var actual = toZ(currentTimestamp);
                assertTrue(nowBeforeTransaction.compareTo(actual) <= 0 && actual.compareTo(nowAfterTransaction) <= 0);
            }
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
            assertEquals(toZ(value(1)).toLocalDateTime(), record.getTimePoint("value")); // TODO record.getTimePointWithTimeZone("value");
        }
    }

    @Test
    @Disabled // TODO remove Disabled. KVS put for timestamp with time zone
    void kvsPut() throws Exception {
        var expected = OffsetDateTime.of(2023, 5, 28, 12, 34, 56, 789_000_001, ZoneOffset.ofHours(9));

        var session = getSession();
        try (var client = KvsClient.attach(session.getLowSession()); // TODO Iceaxe KVS
                var txHandle = client.beginTransaction().await(10, TimeUnit.SECONDS)) {
            var record = new RecordBuffer().add("pk", 1).add("value", expected);
            var result = client.put(txHandle, TEST, record).await(10, TimeUnit.SECONDS);
            assertEquals(1, result.size());

            client.commit(txHandle).await(10, TimeUnit.SECONDS);
        }

        String sql = "select value from " + TEST + " where pk=1";
        var resultMapping = TgResultMapping.ofSingle(OffsetDateTime.class);
        var tm = createTransactionManagerOcc(session);
        var result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(expected, result);
    }
}
