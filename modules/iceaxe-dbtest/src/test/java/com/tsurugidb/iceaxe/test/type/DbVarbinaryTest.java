package com.tsurugidb.iceaxe.test.type;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

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
 * varbinary test
 */
class DbVarbinaryTest extends DbTestTableTester {

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
                + "  value varbinary(10)" //
                + ")";
        var session = getSession();
        executeDdl(session, sql);
    }

    private static void insert(int size) throws IOException, InterruptedException {
        var session = getSession();
        var insertSql = "insert into " + TEST + " values(:pk, :value)";
        var insertMapping = TgParameterMapping.of(TgBindVariables.of().addInt("pk").addBytes("value"));
        try (var ps = session.createStatement(insertSql, insertMapping)) {
            var tm = createTransactionManagerOcc(session);
            tm.execute(transaction -> {
                for (int i = 0; i < size; i++) {
                    var parameter = TgBindParameters.of().addInt("pk", i).addBytes("value", value(size, i));
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
                var value = value(size, i);
                var insertSql = "insert into " + TEST + " values(" + i + ", " + toLiteral(value) + ")";
                try (var ps = session.createStatement(insertSql)) {
                    transaction.executeAndGetCount(ps);
                }
            }
            return;
        });
    }

    private static byte[] value(int size, int i) {
        byte[] buf = new byte[size - i];
        Arrays.fill(buf, (byte) (i + 1));
        return buf;
    }

    @Test
    void tableMetadata() throws Exception {
        var session = getSession();
        var metadata = session.findTableMetadata(TEST).get();
        var list = metadata.getColumnList();
        assertEquals(2, list.size());
        assertColumn("pk", TgDataType.INT, "INT", list.get(0));
        assertColumn("value", TgDataType.BYTES, "BINARY", list.get(1)); // TODO VARBINARY
    }

    private static void assertColumn(String name, TgDataType type, String sqlType, TgSqlColumn actual) {
        assertEquals(name, actual.getName());
        assertEquals(type, actual.getDataType());
        assertEquals(sqlType, actual.getSqlType());
    }

    @ParameterizedTest
    @ValueSource(strings = { "00000000", "ffffffff", "123400abcdef", "", "null" })
    void value(String s) throws Exception {
        var expected = toBytes(s);

        var variable = TgBindVariable.ofBytes("value");
        var updateSql = "update " + TEST + " set value=" + variable + " where pk=1";
        var updateMapping = TgParameterMapping.of(variable);
        var updateParameter = TgBindParameters.of(variable.bind(expected));

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        int count = tm.executeAndGetCount(updateSql, updateMapping, updateParameter);
        assertEquals(1, count);

        var actual = tm.executeAndFindRecord("select * from " + TEST + " where pk=1").get();
        assertArrayEquals(expected, actual.getBytesOrNull("value"));
    }

    @ParameterizedTest
    @ValueSource(strings = { "00000000", "ffffffff", "123400abcdef", "", "null" })
    void valueLiteral(String s) throws Exception {
        var expected = toBytes(s);

        var literal = s.equals("null") ? "null" : "X'" + s + "'";
        var updateSql = "update " + TEST + " set value=" + literal + " where pk=1";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        int count = tm.executeAndGetCount(updateSql);
        assertEquals(1, count);

        var actual = tm.executeAndFindRecord("select * from " + TEST + " where pk=1").get();
        assertArrayEquals(expected, actual.getBytesOrNull("value"));
    }

    private static byte[] toBytes(String s) {
        if (s.equals("null")) {
            return null;
        }

        var buf = new byte[s.length() / 2];
        for (int i = 0; i < buf.length; i++) {
            buf[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
        }
        return buf;
    }

    @Test
    void bindWhereEq() throws Exception {
        var variable = TgBindVariable.ofBytes("value");
        var sql = "select * from " + TEST + " where value=" + variable;
        var mapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, mapping)) {
            var value = value(SIZE, 2);
            var parameter = TgBindParameters.of(variable.bind(value));
            var entity = tm.executeAndFindRecord(ps, parameter).get();
            assertArrayEquals(value, entity.getBytes("value"));
        }
    }

    @Test
    void bindWhereRange() throws Exception {
        var start = TgBindVariable.ofBytes("start");
        var end = TgBindVariable.ofBytes("end");
        var sql = "select * from " + TEST + " where " + start + "<=value and value<=" + end + " order by value";
        var mapping = TgParameterMapping.of(start, end);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, mapping)) {
            var parameter = TgBindParameters.of(start.bind(value(SIZE, 2)), end.bind(value(SIZE, 3)));
            var list = tm.executeAndGetList(ps, parameter);
            assertEquals(2, list.size());
            assertArrayEquals(value(SIZE, 2), list.get(0).getBytes("value"));
            assertArrayEquals(value(SIZE, 3), list.get(1).getBytes("value"));
        }
    }

    @Test
    void whereEq() throws Exception {
        var session = getSession();
        var value = value(SIZE, 2);
        var sql = "select * from " + TEST + " where value=" + toLiteral(value);
        var tm = createTransactionManagerOcc(session);
        var entity = tm.executeAndFindRecord(sql).get();
        assertArrayEquals(value, entity.getBytes("value"));
    }

    private static String toLiteral(byte[] s) {
        var sb = new StringBuilder(s.length * 2 + 3);
        sb.append("X'");
        for (byte b : s) {
            sb.append(String.format("%02x", b));
        }
        sb.append("'");
        return sb.toString();
    }

    @Test
    @Disabled // TODO implicit conversion: char to varbinary
    void implicitConversion() throws Exception {
        var session = getSession();
        String sql = "select * from " + TEST + " where value = '0404'";
        var tm = createTransactionManagerOcc(session);
        TsurugiResultEntity entity = tm.executeAndFindRecord(sql).get();
        assertArrayEquals(new byte[] { 4, 4 }, entity.getBytes("value"));
    }

    @Test
    void cast() throws Exception {
        var session = getSession();
        String sql = "update " + TEST + " set value = cast('1234' as varbinary(10))";
        var tm = createTransactionManagerOcc(session);
        int count = tm.executeAndGetCount(sql);
        assertEquals(SIZE, count);

        var list = tm.executeAndGetList("select * from " + TEST);
        assertEquals(SIZE, list.size());
        for (var actual : list) {
            assertArrayEquals(new byte[] { 0x12, 0x34 }, actual.getBytes("value"));
        }
    }

    @Test
    void min() throws Exception {
        var session = getSession();
        String sql = "select min(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(byte[].class);
        var tm = createTransactionManagerOcc(session);
        byte[] result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertArrayEquals(value(SIZE, 0), result);
    }

    @Test
    void max() throws Exception {
        var session = getSession();
        String sql = "select max(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(byte[].class);
        var tm = createTransactionManagerOcc(session);
        byte[] result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertArrayEquals(value(SIZE, SIZE - 1), result);
    }

    @Test
    void sum() throws Exception {
        var session = getSession();
        String sql = "select sum(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(String.class);
        var tm = createTransactionManagerOcc(session);
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            tm.executeAndFindRecord(sql, resultMapping);
        });
        assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e);
        assertContains("compile failed with error:function_not_found message:\"set function not found: sum(octet-varying(10))\"", e.getMessage());
    }

    @Test
    @Disabled // TODO remove Disabled. KVS varbinary
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
            assertArrayEquals(value(SIZE, 1), record.getOctet("value"));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 2 })
    @Disabled // TODO remove Disabled. KVS varbinary
    void kvsPut(int n) throws Exception {
        byte[] expected = value(SIZE, n);

        var session = getSession();
        try (var client = KvsClient.attach(session.getLowSession()); // TODO Iceaxe KVS
                var txHandle = client.beginTransaction().await(10, TimeUnit.SECONDS)) {
            var record = new RecordBuffer().add("pk", 1).add("value", expected);
            var result = client.put(txHandle, TEST, record).await(10, TimeUnit.SECONDS);
            assertEquals(1, result.size());

            client.commit(txHandle).await(10, TimeUnit.SECONDS);
        }

        String sql = "select value from " + TEST + " where pk=1";
        var resultMapping = TgResultMapping.ofSingle(byte[].class);
        var tm = createTransactionManagerOcc(session);
        byte[] result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertArrayEquals(expected, result);
    }
}
