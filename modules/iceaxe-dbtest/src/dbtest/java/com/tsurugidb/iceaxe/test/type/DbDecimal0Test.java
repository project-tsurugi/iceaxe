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

import java.io.IOException;
import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;
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
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;

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
                    var parameter = TgBindParameters.of().addInt("pk", i).addDecimal("value", value(size, i));
                    transaction.executeAndGetCount(ps, parameter);
                }
                return;
            });
        }
    }

    private static BigDecimal value(int size, int i) {
        return BigDecimal.valueOf(size - i - 1);
    }

    @Test
    void tableMetadata() throws Exception {
        var session = getSession();
        var metadata = session.findTableMetadata(TEST).get();
        var list = metadata.getColumnList();
        assertEquals(2, list.size());
        assertColumn("pk", TgDataType.INT, "INT", list.get(0));
        assertColumn("value", TgDataType.DECIMAL, "DECIMAL(5, 0)", list.get(1));
    }

    private static void assertColumn(String name, TgDataType type, String sqlType, TgSqlColumn actual) {
        assertEquals(name, actual.getName());
        assertEquals(type, actual.getDataType());
        assertEquals(sqlType, actual.getSqlType());
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
            assertEquals(value(SIZE, 1), record.getDecimal("value"));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 123, 0, -1 })
    void kvsPut(int n) throws Exception {
        var expected = BigDecimal.valueOf(n);

        var session = getSession();
        try (var client = KvsClient.attach(session.getLowSession()); // TODO Iceaxe KVS
                var txHandle = client.beginTransaction().await(10, TimeUnit.SECONDS)) {
            var record = new RecordBuffer().add("pk", 1).add("value", expected);
            var result = client.put(txHandle, TEST, record).await(10, TimeUnit.SECONDS);
            assertEquals(1, result.size());

            client.commit(txHandle).await(10, TimeUnit.SECONDS);
        }

        String sql = "select value from " + TEST + " where pk=1";
        var resultMapping = TgResultMapping.ofSingle(BigDecimal.class);
        var tm = createTransactionManagerOcc(session);
        var result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(expected, result);
    }
}
