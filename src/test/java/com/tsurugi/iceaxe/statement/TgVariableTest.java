package com.tsurugi.iceaxe.statement;

import static com.tsurugi.iceaxe.statement.TgDataType.*;
import static org.junit.jupiter.api.Assertions.*;
import static java.util.Map.entry;

import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.protos.CommonProtos.DataType;

class TgVariableTest {

    @Test
    void testOf() {
        var variable = TgVariable.of();
        assertVariable(Map.of(), variable);
        assertEquals("TgVariable[]", variable.toString());
    }

    @Test
    void testInt4() {
        var variable = new TgVariable().int4("foo");
        assertVariable(Map.of("foo", INT4), variable);
    }

    @Test
    void testInt8() {
        var variable = new TgVariable().int8("foo");
        assertVariable(Map.of("foo", INT8), variable);
    }

    @Test
    void testFloat4() {
        var variable = new TgVariable().float4("foo");
        assertVariable(Map.of("foo", FLOAT4), variable);
    }

    @Test
    void testFloat8() {
        var variable = new TgVariable().float8("foo");
        assertVariable(Map.of("foo", FLOAT8), variable);
    }

    @Test
    void testCharacter() {
        var variable = new TgVariable().character("foo");
        assertVariable(Map.of("foo", CHARACTER), variable);
    }

    @Test
    void testSetDataType() {
        var variable = new TgVariable() //
                .set("i4", TgDataType.INT4) //
                .set("i8", TgDataType.INT8) //
                .set("f4", TgDataType.FLOAT4) //
                .set("f8", TgDataType.FLOAT8) //
                .set("c", TgDataType.CHARACTER);
        assertVariable(Map.ofEntries( //
                entry("i4", INT4), //
                entry("i8", INT8), //
                entry("f4", FLOAT4), //
                entry("f8", FLOAT8), //
                entry("c", CHARACTER)), variable);
    }

    @Test
    void testSetClass() {
        var variable = new TgVariable() //
                .set("i4", int.class) //
                .set("I4", Integer.class) //
                .set("i8", long.class) //
                .set("I8", Long.class) //
                .set("f4", float.class) //
                .set("F4", Float.class) //
                .set("f8", double.class) //
                .set("F8", Double.class) //
                .set("c", String.class);
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

    private static void assertVariable(Map<String, TgDataType> expectedMap, TgVariable actual) {
        var ph = actual.toLowPlaceHolder();
        var actualMap = ph.getVariablesList().stream().collect(Collectors.toMap(v -> v.getName(), v -> v.getType()));
        assertEquals(expectedMap.size(), actualMap.size());
        expectedMap.forEach((name, expectedType) -> {
            DataType actualType = actualMap.get(name);
            assertEquals(expectedType.getLowDataType(), actualType);
        });
    }
}
