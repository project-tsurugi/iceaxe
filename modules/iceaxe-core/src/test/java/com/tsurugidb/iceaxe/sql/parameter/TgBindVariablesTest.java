/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.iceaxe.sql.parameter;

import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.type.TgBlob;
import com.tsurugidb.iceaxe.sql.type.TgClob;

class TgBindVariablesTest {

    @Test
    void of() {
        var variables = TgBindVariables.of();
        assertVariable(Map.of(), variables);
        assertEquals("TgBindVariables[]", variables.toString());
    }

    @Test
    void ofArray() {
        var v1 = TgBindVariable.ofInt("foo");
        var variables = TgBindVariables.of(v1);
        assertVariable(Map.of("foo", TgDataType.INT), variables);
    }

    @Test
    void ofCollection() {
        var v1 = TgBindVariable.ofInt("foo");
        var v2 = TgBindVariable.ofLong("bar");
        var variables = TgBindVariables.of(v1, v2);
        assertVariable(Map.of("foo", TgDataType.INT, "bar", TgDataType.LONG), variables);
    }

    @Test
    void toSqlNamesArray() {
        var v1 = TgBindVariable.ofInt("foo");
        var v2 = TgBindVariable.ofLong("bar");
        assertEquals(":foo,:bar", TgBindVariables.toSqlNames(v1, v2));
        assertEquals(":foo, :bar", TgBindVariables.toSqlNames(", ", v1, v2));
    }

    @Test
    void toSqlNamesCollection() {
        var v1 = TgBindVariable.ofInt("foo");
        var v2 = TgBindVariable.ofLong("bar");
        var list = List.of(v1, v2);
        assertEquals(":foo,:bar", TgBindVariables.toSqlNames(list));
        assertEquals(":foo, :bar", TgBindVariables.toSqlNames(", ", list));
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
    void testBlob() {
        var variables = new TgBindVariables().addBlob("foo");
        assertVariable(Map.of("foo", TgDataType.BLOB), variables);
    }

    // TODO CLOB

    @Test
    void testSetDataType() {
        var variables = new TgBindVariables() //
                .add("i4", TgDataType.INT) //
                .add("i8", TgDataType.LONG) //
                .add("f4", TgDataType.FLOAT) //
                .add("f8", TgDataType.DOUBLE) //
                .add("c", TgDataType.STRING) //
                .add("bytes", TgDataType.BYTES) //
                .add("bits", TgDataType.BITS) //
                .add("date", TgDataType.DATE) //
                .add("time", TgDataType.TIME) //
                .add("dateTime", TgDataType.DATE_TIME) //
                .add("offsetTime", TgDataType.OFFSET_TIME) //
                .add("offsetDateTime", TgDataType.OFFSET_DATE_TIME) //
                .add("zonedDateTime", TgDataType.ZONED_DATE_TIME) //
                .add("blob", TgDataType.BLOB) //
                .add("clob", TgDataType.CLOB) //
        ;
        assertVariable(Map.ofEntries( //
                entry("i4", TgDataType.INT), //
                entry("i8", TgDataType.LONG), //
                entry("f4", TgDataType.FLOAT), //
                entry("f8", TgDataType.DOUBLE), //
                entry("c", TgDataType.STRING), //
                entry("bytes", TgDataType.BYTES), //
                entry("bits", TgDataType.BITS), //
                entry("date", TgDataType.DATE), //
                entry("time", TgDataType.TIME), //
                entry("dateTime", TgDataType.DATE_TIME), //
                entry("offsetTime", TgDataType.OFFSET_TIME), //
                entry("offsetDateTime", TgDataType.OFFSET_DATE_TIME), //
                entry("zonedDateTime", TgDataType.ZONED_DATE_TIME), //
                entry("blob", TgDataType.BLOB), //
                entry("clob", TgDataType.CLOB) //
        ), variables);
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
                .add("c", String.class) //
                .add("bytes", byte[].class) //
                .add("bits", boolean[].class) //
                .add("date", LocalDate.class) //
                .add("time", LocalTime.class) //
                .add("dateTime", LocalDateTime.class) //
                .add("offsetTime", OffsetTime.class) //
                .add("offsetDateTime", OffsetDateTime.class) //
                .add("zonedDateTime", ZonedDateTime.class) //
                .add("blob", TgBlob.class) //
                .add("clob", TgClob.class) //
        ;
        assertVariable(Map.ofEntries( //
                entry("i4", TgDataType.INT), //
                entry("I4", TgDataType.INT), //
                entry("i8", TgDataType.LONG), //
                entry("I8", TgDataType.LONG), //
                entry("f4", TgDataType.FLOAT), //
                entry("F4", TgDataType.FLOAT), //
                entry("f8", TgDataType.DOUBLE), //
                entry("F8", TgDataType.DOUBLE), //
                entry("c", TgDataType.STRING), //
                entry("bytes", TgDataType.BYTES), //
                entry("bits", TgDataType.BITS), //
                entry("date", TgDataType.DATE), //
                entry("time", TgDataType.TIME), //
                entry("dateTime", TgDataType.DATE_TIME), //
                entry("offsetTime", TgDataType.OFFSET_TIME), //
                entry("offsetDateTime", TgDataType.OFFSET_DATE_TIME), //
                entry("zonedDateTime", TgDataType.ZONED_DATE_TIME), //
                entry("blob", TgDataType.BLOB), //
                entry("clob", TgDataType.CLOB) //
        ), variables);
    }

    @Test
    void addTgVariables() {
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
    void getSqlNames() {
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
