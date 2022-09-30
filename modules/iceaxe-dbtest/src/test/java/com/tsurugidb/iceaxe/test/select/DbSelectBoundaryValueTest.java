package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * select boundary value test
 */
class DbSelectBoundaryValueTest extends DbTestTableTester {
    private static final Logger LOG = LoggerFactory.getLogger(DbSelectBoundaryValueTest.class);

    @BeforeAll
    static void beforeAll(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTable();
        insertTable();

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

    private static class TestEntity {
        private int int4;
        private long int8;
        private float float4;
        private double float8;

        public TestEntity() {
        }

        public TestEntity(int int4, long int8, float float4, double float8) {
            this.int4 = int4;
            this.int8 = int8;
            this.float4 = float4;
            this.float8 = float8;
        }

        public void setInt4(int int4) {
            this.int4 = int4;
        }

        public int getInt4() {
            return this.int4;
        }

        public void setInt8(long int8) {
            this.int8 = int8;
        }

        public long getInt8() {
            return this.int8;
        }

        public void setFloat4(float float4) {
            this.float4 = float4;
        }

        public float getFloat4() {
            return this.float4;
        }

        public void setFloat8(double float8) {
            this.float8 = float8;
        }

        public double getFloat8() {
            return this.float8;
        }

        @Override
        public int hashCode() {
            return Objects.hash(int4, int8, float4, float8);
        }

        @Override
        public boolean equals(Object obj) {
            TestEntity other = (TestEntity) obj;
            return int4 == other.int4 && int8 == other.int8 && Float.floatToIntBits(float4) == Float.floatToIntBits(other.float4)
                    && Double.doubleToLongBits(float8) == Double.doubleToLongBits(other.float8);
        }
    }

    private static final List<TestEntity> TEST_ENTITY_LIST = List.of(//
            new TestEntity(Integer.MIN_VALUE, Long.MIN_VALUE, Float.MIN_VALUE, Double.MIN_VALUE), //
            new TestEntity(Integer.MIN_VALUE + 1, Long.MIN_VALUE + 1, Float.MIN_VALUE + Float.MIN_NORMAL, Double.MIN_VALUE + Double.MIN_NORMAL), //
            new TestEntity(Integer.MIN_VALUE + 2, Long.MIN_VALUE + 2, Float.MIN_VALUE + Float.MIN_NORMAL * 2, Double.MIN_VALUE + Double.MIN_NORMAL * 2), //
            new TestEntity(Integer.MAX_VALUE - 2, Long.MAX_VALUE - 2, Float.MAX_VALUE - Float.MIN_NORMAL * 2, Double.MAX_VALUE - Double.MIN_NORMAL * 2), //
            new TestEntity(Integer.MAX_VALUE - 1, Long.MAX_VALUE - 1, Float.MAX_VALUE - Float.MIN_NORMAL, Double.MAX_VALUE - Double.MIN_NORMAL), //
            new TestEntity(Integer.MAX_VALUE, Long.MAX_VALUE, Float.MAX_VALUE, Double.MAX_VALUE) //
    );

    private static void insertTable() throws IOException {
        var sql = "insert into " + TEST //
                + "(int4, int8, float4, float8)" //
                + "values(:int4, :int8, :float4, :float8)";
        var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                .int4("int4", TestEntity::getInt4) //
                .int8("int8", TestEntity::getInt8) //
                .float4("float4", TestEntity::getFloat4) //
                .float8("float8", TestEntity::getFloat8);

        var session = getSession();
        var tm = createTransactionManagerOcc(session, 3);
        try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
            tm.execute(transaction -> {
                for (var entity : TEST_ENTITY_LIST) {
                    ps.executeAndGetCount(transaction, entity);
                }
            });
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, Integer.MIN_VALUE + 1, Integer.MAX_VALUE - 1, Integer.MAX_VALUE })
    void selectInt4(int value) throws IOException {
        var variable = TgVariable.ofInt4("value");
        test("int4 =:value", entity -> entity.getInt4() == value, value, variable);
        test("int4<>:value", entity -> entity.getInt4() != value, value, variable);
        test("int4>=:value", entity -> entity.getInt4() >= value, value, variable);
        test("int4> :value", entity -> entity.getInt4() > value, value, variable);
        test("int4<=:value", entity -> entity.getInt4() <= value, value, variable);
        test("int4< :value", entity -> entity.getInt4() < value, value, variable);
    }

    @ParameterizedTest
    @ValueSource(longs = { Long.MIN_VALUE, Long.MIN_VALUE + 1, Long.MAX_VALUE - 1, Long.MAX_VALUE })
    void selectInt8(long value) throws IOException {
        var variable = TgVariable.ofInt8("value");
        test("int8 =:value", entity -> entity.getInt8() == value, value, variable);
        test("int8<>:value", entity -> entity.getInt8() != value, value, variable);
        test("int8>=:value", entity -> entity.getInt8() >= value, value, variable);
        test("int8> :value", entity -> entity.getInt8() > value, value, variable);
        test("int8<=:value", entity -> entity.getInt8() <= value, value, variable);
        test("int8< :value", entity -> entity.getInt8() < value, value, variable);
    }

    @ParameterizedTest
    @ValueSource(floats = { Float.MIN_VALUE, Float.MIN_VALUE + Float.MIN_NORMAL, Float.MAX_VALUE - Float.MIN_NORMAL, Float.MAX_VALUE, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, Float.NaN })
    void selectFloat4(float value) throws IOException {
        var variable = TgVariable.ofFloat4("value");
        test("float4 =:value", entity -> entity.getFloat4() == value, value, variable);
        test("float4<>:value", entity -> entity.getFloat4() != value, value, variable);
        test("float4>=:value", entity -> entity.getFloat4() >= value, value, variable);
        test("float4> :value", entity -> entity.getFloat4() > value, value, variable);
        test("float4<=:value", entity -> entity.getFloat4() <= value, value, variable);
        test("float4< :value", entity -> entity.getFloat4() < value, value, variable);
    }

    @ParameterizedTest
    @ValueSource(doubles = { Double.MIN_VALUE, Double.MIN_VALUE + Double.MIN_NORMAL, Double.MIN_VALUE - Double.MIN_NORMAL, Double.MAX_VALUE, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY,
            Double.NaN })
    void selectFloat8(double value) throws IOException {
        var variable = TgVariable.ofFloat8("value");
        test("float8 =:value", entity -> entity.getFloat8() == value, value, variable);
        test("float8<>:value", entity -> entity.getFloat8() != value, value, variable);
        test("float8>=:value", entity -> entity.getFloat8() >= value, value, variable);
        test("float8> :value", entity -> entity.getFloat8() > value, value, variable);
        test("float8<=:value", entity -> entity.getFloat8() <= value, value, variable);
        test("float8< :value", entity -> entity.getFloat8() < value, value, variable);
    }

    private static <T> void test(String where, Predicate<TestEntity> expectedPredicate, T value, TgVariable<T> variable) throws IOException {
        var sql = "select int4, int8, float4, float8 from " + TEST //
                + " where " + where //
                + " order by int4";
        var parameterMapping = TgParameterMapping.of(variable);
        var resultMapping = TgResultMapping.of(TestEntity::new) //
                .int4("int4", TestEntity::setInt4) //
                .int8("int8", TestEntity::setInt8) //
                .float4("float4", TestEntity::setFloat4) //
                .float8("float8", TestEntity::setFloat8);

        var expected = TEST_ENTITY_LIST.stream().filter(expectedPredicate).collect(Collectors.toList());

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, parameterMapping, resultMapping)) {
            var param = TgParameterList.of(variable.bind(value));
            var actual = ps.executeAndGetList(tm, param);
            assertEquals(expected, actual);
        }
    }
}
