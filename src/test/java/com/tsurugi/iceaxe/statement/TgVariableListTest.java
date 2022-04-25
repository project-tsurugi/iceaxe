package com.tsurugi.iceaxe.statement;

import static com.tsurugi.iceaxe.statement.TgDataType.*;
import static org.junit.jupiter.api.Assertions.*;
import static java.util.Map.entry;

import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.protos.CommonProtos.DataType;

class TgVariableListTest {

    @Test
    void testOf() {
        var variable = TgVariableList.of();
        assertVariable(Map.of(), variable);
        assertEquals("TgVariableList[]", variable.toString());
    }

    @Test
    void testInt4() {
        var variable = new TgVariableList().int4("foo");
        assertVariable(Map.of("foo", INT4), variable);
    }

    @Test
    void testInt8() {
        var variable = new TgVariableList().int8("foo");
        assertVariable(Map.of("foo", INT8), variable);
    }

    @Test
    void testFloat4() {
        var variable = new TgVariableList().float4("foo");
        assertVariable(Map.of("foo", FLOAT4), variable);
    }

    @Test
    void testFloat8() {
        var variable = new TgVariableList().float8("foo");
        assertVariable(Map.of("foo", FLOAT8), variable);
    }

    @Test
    void testCharacter() {
        var variable = new TgVariableList().character("foo");
        assertVariable(Map.of("foo", CHARACTER), variable);
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
                entry("i4", INT4), //
                entry("i8", INT8), //
                entry("f4", FLOAT4), //
                entry("f8", FLOAT8), //
                entry("c", CHARACTER)), variable);
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
                entry("i4", INT4), //
                entry("I4", INT4), //
                entry("i8", INT8), //
                entry("I8", INT8), //
                entry("f4", FLOAT4), //
                entry("F4", FLOAT4), //
                entry("f8", FLOAT8), //
                entry("F8", FLOAT8), //
                entry("c", CHARACTER)), variable);
    }

    private static void assertVariable(Map<String, TgDataType> expectedMap, TgVariableList actual) {
        var ph = actual.toLowPlaceHolder();
        var actualMap = ph.getVariablesList().stream().collect(Collectors.toMap(v -> v.getName(), v -> v.getType()));
        assertEquals(expectedMap.size(), actualMap.size());
        expectedMap.forEach((name, expectedType) -> {
            DataType actualType = actualMap.get(name);
            assertEquals(expectedType.getLowDataType(), actualType);
        });
    }
}
