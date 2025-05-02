package com.tsurugidb.iceaxe.test.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * mod function test
 */
class DbModTest extends DbTestTableTester {

    private static void createTable(String type) throws IOException, InterruptedException {
        dropTestTable();

        String sql = "create table " + TEST + "(" //
                + "  pk int primary key," //
                + "  value1 " + type + ", " //
                + "  value2 " + type //
                + ")";
        executeDdl(getSession(), sql);

        int i = 0;
        switch (type) {
        case "int":
        case "bigint":
        case "decimal(10)":
            insert(i++, "null");
            insert(i++, "10");
            insert(i++, "0");
            insert(i++, "-10");
            break;
        case "real":
        case "double":
        case "decimal(10, 1)":
            insert(i++, "null");
            insert(i++, "10");
            insert(i++, "0");
            insert(i++, "-10");
            insert(i++, "10.5");
            insert(i++, "0");
            insert(i++, "-10.5");
            break;
        default:
            throw new AssertionError(type);
        }
    }

    private static void insert(int pk, String value) throws IOException, InterruptedException {
        var session = getSession();
        var insertSql = "insert or replace into " + TEST + " values(" + pk + ", " + value + ", 3)";
        try (var ps = session.createStatement(insertSql)) {
            var tm = createTransactionManagerOcc(session);
            tm.execute(transaction -> {
                transaction.executeAndGetCount(ps);
            });
        }
    }

    @Test
    void nullTest2() throws Exception {
        createTable("int");

        var sql = "select mod(null, null) from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var list = tm.executeAndGetList(sql);
        for (var entity : list) {
            Object value = entity.getValueOrNull(0);
            assertNull(value);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "int", "bigint", "real", "double", "decimal(10)", "decimal(10, 1)" })
    void nullTest1(String type) throws Exception {
        createTable("int");

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        {
            var sql = "select mod(value1, null) from " + TEST;
            var list = tm.executeAndGetList(sql);
            for (var entity : list) {
                Object value = entity.getValueOrNull(0);
                assertNull(value);
            }
        }
        {
            var sql = "select mod(null, value2) from " + TEST;
            var list = tm.executeAndGetList(sql);
            for (var entity : list) {
                Object value = entity.getValueOrNull(0);
                assertNull(value);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "int", "bigint", /* "real", "double", */ "decimal(10)", "decimal(10, 1)" })
    void test(String type) throws Exception {
        createTable(type);

        var sql = "select value1, value2, mod(value1, value2), mod(value1, 3), value1 % value2 from " + TEST;

        var tm = createTransactionManagerOcc(getSession());
        tm.executeAndForEach(sql, entity -> {
            switch (type) {
            case "int": {
                var expected = mod(entity.getIntOrNull(0), entity.getIntOrNull(1));
                assertEquals(expected, entity.getIntOrNull(2));
                assertEquals(expected, entity.getIntOrNull(3));
                assertEquals(expected, entity.getIntOrNull(4));
                break;
            }
            case "bigint": {
                var expected = mod(entity.getLongOrNull(0), entity.getLongOrNull(1));
                assertEquals(expected, entity.getLongOrNull(2));
                assertEquals(expected, entity.getLongOrNull(3));
                assertEquals(expected, entity.getLongOrNull(4));
                break;
            }
            case "real": {
                var expected = mod(entity.getFloatOrNull(0), entity.getFloatOrNull(1));
                assertEquals(expected, entity.getFloatOrNull(2));
                assertEquals(expected, entity.getFloatOrNull(3));
                assertEquals(expected, entity.getFloatOrNull(4));
                break;
            }
            case "double": {
                var expected = mod(entity.getDoubleOrNull(0), entity.getDoubleOrNull(1));
                assertEquals(expected, entity.getDoubleOrNull(2));
                assertEquals(expected, entity.getDoubleOrNull(3));
                assertEquals(expected, entity.getDoubleOrNull(4));
                break;
            }
            case "decimal(10)":
            case "decimal(10, 1)": {
                var expected = mod(entity.getDecimalOrNull(0), entity.getDecimalOrNull(1));
                if (expected == null) {
                    assertNull(entity.getDecimalOrNull(2));
                    assertNull(entity.getDecimalOrNull(3));
                    assertNull(entity.getDecimalOrNull(4));
                } else {
                    assertTrue(expected.compareTo(entity.getDecimalOrNull(2)) == 0);
                    assertTrue(expected.compareTo(entity.getDecimalOrNull(3)) == 0);
                    assertTrue(expected.compareTo(entity.getDecimalOrNull(4)) == 0);
                }
                break;
            }
            default:
                throw new AssertionError(type);
            }
        });
    }

    private Integer mod(Integer value1, Integer value2) {
        if (value1 == null || value2 == null) {
            return null;
        }
        return value1 % value2;
    }

    private Long mod(Long value1, Long value2) {
        if (value1 == null || value2 == null) {
            return null;
        }
        return value1 % value2;
    }

    private Float mod(Float value1, Float value2) {
        if (value1 == null || value2 == null) {
            return null;
        }
        return value1 % value2;
    }

    private Double mod(Double value1, Double value2) {
        if (value1 == null || value2 == null) {
            return null;
        }
        return value1 % value2;
    }

    private BigDecimal mod(BigDecimal value1, BigDecimal value2) {
        if (value1 == null || value2 == null) {
            return null;
        }
        return BigDecimal.valueOf(value1.doubleValue() % value2.doubleValue());
    }
}
