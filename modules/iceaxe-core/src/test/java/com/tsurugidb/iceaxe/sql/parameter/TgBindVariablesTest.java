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
        var variable = TgBindVariables.of();
        assertVariable(Map.of(), variable);
        assertEquals("TgBindVariables[]", variable.toString());
    }

    @Test
    void testBoolean() {
        var variable = new TgBindVariables().addBoolean("foo");
        assertVariable(Map.of("foo", TgDataType.BOOLEAN), variable);
    }

    @Test
    void testInt() {
        var variable = new TgBindVariables().addInt("foo");
        assertVariable(Map.of("foo", TgDataType.INT), variable);
    }

    @Test
    void testLong() {
        var variable = new TgBindVariables().addLong("foo");
        assertVariable(Map.of("foo", TgDataType.LONG), variable);
    }

    @Test
    void testFloat() {
        var variable = new TgBindVariables().addFloat("foo");
        assertVariable(Map.of("foo", TgDataType.FLOAT), variable);
    }

    @Test
    void testDouble() {
        var variable = new TgBindVariables().addDouble("foo");
        assertVariable(Map.of("foo", TgDataType.DOUBLE), variable);
    }

    @Test
    void testDecimal() {
        var variable = new TgBindVariables().addDecimal("foo");
        assertVariable(Map.of("foo", TgDataType.DECIMAL), variable);
    }

    @Test
    void testString() {
        var variable = new TgBindVariables().addString("foo");
        assertVariable(Map.of("foo", TgDataType.STRING), variable);
    }

    @Test
    void testBytes() {
        var variable = new TgBindVariables().addBytes("foo");
        assertVariable(Map.of("foo", TgDataType.BYTES), variable);
    }

    @Test
    void testBits() {
        var variable = new TgBindVariables().addBits("foo");
        assertVariable(Map.of("foo", TgDataType.BITS), variable);
    }

    @Test
    void testDate() {
        var variable = new TgBindVariables().addDate("foo");
        assertVariable(Map.of("foo", TgDataType.DATE), variable);
    }

    @Test
    void testTime() {
        var variable = new TgBindVariables().addTime("foo");
        assertVariable(Map.of("foo", TgDataType.TIME), variable);
    }

    @Test
    void testDateTime() {
        var variable = new TgBindVariables().addDateTime("foo");
        assertVariable(Map.of("foo", TgDataType.DATE_TIME), variable);
    }

    @Test
    void testOffsetTime() {
        var variable = new TgBindVariables().addOffsetTime("foo");
        assertVariable(Map.of("foo", TgDataType.OFFSET_TIME), variable);
    }

    @Test
    void testOffsetDateTime() {
        var variable = new TgBindVariables().addOffsetDateTime("foo");
        assertVariable(Map.of("foo", TgDataType.OFFSET_DATE_TIME), variable);
    }

    @Test
    void testZonedDateTime() {
        var variable = new TgBindVariables().addZonedDateTime("foo");
        assertVariable(Map.of("foo", TgDataType.OFFSET_DATE_TIME), variable);
    }

    @Test
    void testSetDataType() {
        var variable = new TgBindVariables() //
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
                entry("c", TgDataType.STRING)), variable);
    }

    @Test
    void testSetClass() {
        var variable = new TgBindVariables() //
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
                entry("c", TgDataType.STRING)), variable);
    }

    @Test
    void testAddTgVariableList() {
        var variable1 = new TgBindVariables() //
                .addInt("foo") //
                .addLong("bar");
        var variable = new TgBindVariables() //
                .addString("zzz");
        variable.add(variable1);

        assertVariable(Map.ofEntries( //
                entry("foo", TgDataType.INT), //
                entry("bar", TgDataType.LONG), //
                entry("zzz", TgDataType.STRING)), variable);
        assertVariable(Map.ofEntries( //
                entry("foo", TgDataType.INT), //
                entry("bar", TgDataType.LONG)), variable1);
    }

    @Test
    void testGetSqlNames() {
        var variable = new TgBindVariables() //
                .addInt("foo") //
                .addLong("bar") //
                .addString("zzz");

        assertEquals(":foo,:bar,:zzz", variable.getSqlNames());
        assertEquals(":foo, :bar, :zzz", variable.getSqlNames(", "));
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
