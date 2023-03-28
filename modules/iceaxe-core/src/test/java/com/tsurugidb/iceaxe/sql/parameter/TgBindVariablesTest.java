package com.tsurugidb.iceaxe.sql.parameter;

import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.sql.TgDataType;

class TgBindVariablesTest {

    @Test
    void testOf() {
        var variables = TgBindVariables.of();
        assertVariable(Map.of(), variables);
        assertEquals("TgBindVariables[]", variables.toString());
    }

    @Test
    void testOfArray() {
        var v1 = TgBindVariable.ofInt("foo");
        var variables = TgBindVariables.of(v1);
        assertVariable(Map.of("foo", TgDataType.INT), variables);
    }

    @Test
    void testOfCollection() {
        var v1 = TgBindVariable.ofInt("foo");
        var v2 = TgBindVariable.ofLong("bar");
        var variables = TgBindVariables.of(v1, v2);
        assertVariable(Map.of("foo", TgDataType.INT, "bar", TgDataType.LONG), variables);
    }

    @Test
    void testBoolean() {
        var variables = new TgBindVariables().addBoolean("foo");
        assertVariable(Map.of("foo", TgDataType.BOOLEAN), variables);
    }

    @Test
    void testInt() {
        var variables = new TgBindVariables().addInt("foo");
        assertVariable(Map.of("foo", TgDataType.INT), variables);
    }

    @Test
    void testLong() {
        var variables = new TgBindVariables().addLong("foo");
        assertVariable(Map.of("foo", TgDataType.LONG), variables);
    }

    @Test
    void testFloat() {
        var variables = new TgBindVariables().addFloat("foo");
        assertVariable(Map.of("foo", TgDataType.FLOAT), variables);
    }

    @Test
    void testDouble() {
        var variables = new TgBindVariables().addDouble("foo");
        assertVariable(Map.of("foo", TgDataType.DOUBLE), variables);
    }

    @Test
    void testDecimal() {
        var variables = new TgBindVariables().addDecimal("foo");
        assertVariable(Map.of("foo", TgDataType.DECIMAL), variables);
    }

    @Test
    void testString() {
        var variables = new TgBindVariables().addString("foo");
        assertVariable(Map.of("foo", TgDataType.STRING), variables);
    }

    @Test
    void testBytes() {
        var variables = new TgBindVariables().addBytes("foo");
        assertVariable(Map.of("foo", TgDataType.BYTES), variables);
    }

    @Test
    void testBits() {
        var variables = new TgBindVariables().addBits("foo");
        assertVariable(Map.of("foo", TgDataType.BITS), variables);
    }

    @Test
    void testDate() {
        var variables = new TgBindVariables().addDate("foo");
        assertVariable(Map.of("foo", TgDataType.DATE), variables);
    }

    @Test
    void testTime() {
        var variables = new TgBindVariables().addTime("foo");
        assertVariable(Map.of("foo", TgDataType.TIME), variables);
    }

    @Test
    void testDateTime() {
        var variables = new TgBindVariables().addDateTime("foo");
        assertVariable(Map.of("foo", TgDataType.DATE_TIME), variables);
    }

    @Test
    void testOffsetTime() {
        var variables = new TgBindVariables().addOffsetTime("foo");
        assertVariable(Map.of("foo", TgDataType.OFFSET_TIME), variables);
    }

    @Test
    void testOffsetDateTime() {
        var variables = new TgBindVariables().addOffsetDateTime("foo");
        assertVariable(Map.of("foo", TgDataType.OFFSET_DATE_TIME), variables);
    }

    @Test
    void testZonedDateTime() {
        var variables = new TgBindVariables().addZonedDateTime("foo");
        assertVariable(Map.of("foo", TgDataType.ZONED_DATE_TIME), variables);
    }

    @Test
    void testSetDataType() {
        var variables = new TgBindVariables() //
                .add("i4", TgDataType.INT) //
                .add("i8", TgDataType.LONG) //
                .add("f4", TgDataType.FLOAT) //
                .add("f8", TgDataType.DOUBLE) //
                .add("c", TgDataType.STRING);
        assertVariable(Map.ofEntries( //
                entry("i4", TgDataType.INT), //
                entry("i8", TgDataType.LONG), //
                entry("f4", TgDataType.FLOAT), //
                entry("f8", TgDataType.DOUBLE), //
                entry("c", TgDataType.STRING)), variables);
    }

    @Test
    void testSetClass() {
        var variables = new TgBindVariables() //
                .add("i4", int.class) //
                .add("I4", Integer.class) //
                .add("i8", long.class) //
                .add("I8", Long.class) //
                .add("f4", float.class) //
                .add("F4", Float.class) //
                .add("f8", double.class) //
                .add("F8", Double.class) //
                .add("c", String.class);
        assertVariable(Map.ofEntries( //
                entry("i4", TgDataType.INT), //
                entry("I4", TgDataType.INT), //
                entry("i8", TgDataType.LONG), //
                entry("I8", TgDataType.LONG), //
                entry("f4", TgDataType.FLOAT), //
                entry("F4", TgDataType.FLOAT), //
                entry("f8", TgDataType.DOUBLE), //
                entry("F8", TgDataType.DOUBLE), //
                entry("c", TgDataType.STRING)), variables);
    }

    @Test
    void testAddTgVariabls() {
        var variable1 = new TgBindVariables() //
                .addInt("foo") //
                .addLong("bar");
        var variables = new TgBindVariables() //
                .addString("zzz");
        variables.add(variable1);

        assertVariable(Map.ofEntries( //
                entry("foo", TgDataType.INT), //
                entry("bar", TgDataType.LONG), //
                entry("zzz", TgDataType.STRING)), variables);
        assertVariable(Map.ofEntries( //
                entry("foo", TgDataType.INT), //
                entry("bar", TgDataType.LONG)), variable1);
    }

    @Test
    void testGetSqlNames() {
        var variables = new TgBindVariables() //
                .addInt("foo") //
                .addLong("bar") //
                .addString("zzz");

        assertEquals(":foo,:bar,:zzz", variables.getSqlNames());
        assertEquals(":foo, :bar, :zzz", variables.getSqlNames(", "));
    }

    private static void assertVariable(Map<String, TgDataType> expectedMap, TgBindVariables actual) {
        var phList = actual.toLowPlaceholderList();
        var actualMap = phList.stream().collect(Collectors.toMap(v -> v.getName(), v -> v.getAtomType()));
        assertEquals(expectedMap.size(), actualMap.size());
        expectedMap.forEach((name, expectedType) -> {
            var actualType = actualMap.get(name);
            assertEquals(expectedType.getLowDataType(), actualType);
        });
    }
}
