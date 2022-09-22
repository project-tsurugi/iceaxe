package com.tsurugidb.iceaxe.statement;

import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

class TgVariableListTest {

    @Test
    void testOf() {
        var variable = TgVariableList.of();
        assertVariable(Map.of(), variable);
        assertEquals("TgVariableList[]", variable.toString());
    }

    @Test
    void testBool() {
        var variable = new TgVariableList().bool("foo");
        assertVariable(Map.of("foo", TgDataType.BOOLEAN), variable);
    }

    @Test
    void testInt4() {
        var variable = new TgVariableList().int4("foo");
        assertVariable(Map.of("foo", TgDataType.INT4), variable);
    }

    @Test
    void testInt8() {
        var variable = new TgVariableList().int8("foo");
        assertVariable(Map.of("foo", TgDataType.INT8), variable);
    }

    @Test
    void testFloat4() {
        var variable = new TgVariableList().float4("foo");
        assertVariable(Map.of("foo", TgDataType.FLOAT4), variable);
    }

    @Test
    void testFloat8() {
        var variable = new TgVariableList().float8("foo");
        assertVariable(Map.of("foo", TgDataType.FLOAT8), variable);
    }

    @Test
    void testDecimal() {
        var variable = new TgVariableList().decimal("foo");
        assertVariable(Map.of("foo", TgDataType.DECIMAL), variable);
    }

    @Test
    void testCharacter() {
        var variable = new TgVariableList().character("foo");
        assertVariable(Map.of("foo", TgDataType.CHARACTER), variable);
    }

    @Test
    void testBytes() {
        var variable = new TgVariableList().bytes("foo");
        assertVariable(Map.of("foo", TgDataType.BYTES), variable);
    }

    @Test
    void testBits() {
        var variable = new TgVariableList().bits("foo");
        assertVariable(Map.of("foo", TgDataType.BITS), variable);
    }

    @Test
    void testDate() {
        var variable = new TgVariableList().date("foo");
        assertVariable(Map.of("foo", TgDataType.DATE), variable);
    }

    @Test
    void testTime() {
        var variable = new TgVariableList().time("foo");
        assertVariable(Map.of("foo", TgDataType.TIME), variable);
    }

    @Test
    void testDateTime() {
        var variable = new TgVariableList().dateTime("foo");
        assertVariable(Map.of("foo", TgDataType.DATE_TIME), variable);
    }

    @Test
    void testOffsetTime() {
        var variable = new TgVariableList().offsetTime("foo");
        assertVariable(Map.of("foo", TgDataType.OFFSET_TIME), variable);
    }

    @Test
    void testOffsetDateTime() {
        var variable = new TgVariableList().offsetDateTime("foo");
        assertVariable(Map.of("foo", TgDataType.OFFSET_DATE_TIME), variable);
    }

    @Test
    void testZonedDateTime() {
        var variable = new TgVariableList().zonedDateTime("foo");
        assertVariable(Map.of("foo", TgDataType.OFFSET_DATE_TIME), variable);
    }

    @Test
    void testSetDataType() {
        var variable = new TgVariableList() //
                .add("i4", TgDataType.INT4) //
                .add("i8", TgDataType.INT8) //
                .add("f4", TgDataType.FLOAT4) //
                .add("f8", TgDataType.FLOAT8) //
                .add("c", TgDataType.CHARACTER);
        assertVariable(Map.ofEntries( //
                entry("i4", TgDataType.INT4), //
                entry("i8", TgDataType.INT8), //
                entry("f4", TgDataType.FLOAT4), //
                entry("f8", TgDataType.FLOAT8), //
                entry("c", TgDataType.CHARACTER)), variable);
    }

    @Test
    void testSetClass() {
        var variable = new TgVariableList() //
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
                entry("i4", TgDataType.INT4), //
                entry("I4", TgDataType.INT4), //
                entry("i8", TgDataType.INT8), //
                entry("I8", TgDataType.INT8), //
                entry("f4", TgDataType.FLOAT4), //
                entry("F4", TgDataType.FLOAT4), //
                entry("f8", TgDataType.FLOAT8), //
                entry("F8", TgDataType.FLOAT8), //
                entry("c", TgDataType.CHARACTER)), variable);
    }

    @Test
    void testAddTgVariableList() {
        var variable1 = new TgVariableList() //
                .int4("foo") //
                .int8("bar");
        var variable = new TgVariableList() //
                .character("zzz");
        variable.add(variable1);

        assertVariable(Map.ofEntries( //
                entry("foo", TgDataType.INT4), //
                entry("bar", TgDataType.INT8), //
                entry("zzz", TgDataType.CHARACTER)), variable);
        assertVariable(Map.ofEntries( //
                entry("foo", TgDataType.INT4), //
                entry("bar", TgDataType.INT8)), variable1);
    }

    @Test
    void testGetSqlNames() {
        var variable = new TgVariableList() //
                .int4("foo") //
                .int8("bar") //
                .character("zzz");

        assertEquals(":foo,:bar,:zzz", variable.getSqlNames());
        assertEquals(":foo, :bar, :zzz", variable.getSqlNames(", "));
    }

    private static void assertVariable(Map<String, TgDataType> expectedMap, TgVariableList actual) {
        var phList = actual.toLowPlaceholderList();
        var actualMap = phList.stream().collect(Collectors.toMap(v -> v.getName(), v -> v.getAtomType()));
        assertEquals(expectedMap.size(), actualMap.size());
        expectedMap.forEach((name, expectedType) -> {
            var actualType = actualMap.get(name);
            assertEquals(expectedType.getLowDataType(), actualType);
        });
    }
}
