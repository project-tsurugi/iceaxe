package com.tsurugidb.iceaxe.statement;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.low.sql.Parameters;

class TgVariableTest {

    @Test
    void testOfBoolean() {
        var variable = TgVariable.ofBoolean("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.BOOLEAN, variable.type());
        assertEquals(Parameters.of("foo", true), variable.bind(true).toLowParameter());
    }

    @Test
    void testOfInt4() {
        var variable = TgVariable.ofInt4("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.INT4, variable.type());
        assertEquals(Parameters.of("foo", 123), variable.bind(123).toLowParameter());
    }

    @Test
    void testOfInt8() {
        var variable = TgVariable.ofInt8("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.INT8, variable.type());
        assertEquals(Parameters.of("foo", 123L), variable.bind(123L).toLowParameter());
    }

    @Test
    void testOfFloat4() {
        var variable = TgVariable.ofFloat4("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.FLOAT4, variable.type());
        assertEquals(Parameters.of("foo", 123f), variable.bind(123f).toLowParameter());
    }

    @Test
    void testOfFloat8() {
        var variable = TgVariable.ofFloat8("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.FLOAT8, variable.type());
        assertEquals(Parameters.of("foo", 123d), variable.bind(123d).toLowParameter());
    }

    @Test
    void testOfDecimal() {
        var variable = TgVariable.ofDecimal("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.DECIMAL, variable.type());
        assertEquals(Parameters.of("foo", BigDecimal.valueOf(123)), variable.bind(BigDecimal.valueOf(123)).toLowParameter());
    }

    @Test
    void testOfCharacter() {
        var variable = TgVariable.ofCharacter("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.CHARACTER, variable.type());
        assertEquals(Parameters.of("foo", "abc"), variable.bind("abc").toLowParameter());
    }

    @Test
    void testOfBytes() {
        var variable = TgVariable.ofBytes("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.BYTES, variable.type());
        assertEquals(Parameters.of("foo", new byte[] { 1, 2, 3 }), variable.bind(new byte[] { 1, 2, 3 }).toLowParameter());
    }

    @Test
    void testOfBits() {
        var variable = TgVariable.ofBits("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.BITS, variable.type());
        assertEquals(Parameters.of("foo", new boolean[] { true, false, true }), variable.bind(new boolean[] { true, false, true }).toLowParameter());
    }

    @Test
    void testOfDate() {
        var variable = TgVariable.ofDate("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.DATE, variable.type());
        assertEquals(Parameters.of("foo", LocalDate.of(2022, 6, 2)), variable.bind(LocalDate.of(2022, 6, 2)).toLowParameter());
    }

    @Test
    void testOfTime() {
        var variable = TgVariable.ofTime("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.TIME, variable.type());
        assertEquals(Parameters.of("foo", LocalTime.of(23, 30, 59)), variable.bind(LocalTime.of(23, 30, 59)).toLowParameter());
    }

    @Test
    void testOfInstant() {
        var variable = TgVariable.ofInstant("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.INSTANT, variable.type());

        var zone = ZoneId.of("Asia/Tokyo");
        var instant = ZonedDateTime.of(2022, 6, 2, 23, 30, 59, 999, zone).toInstant();
        assertEquals(Parameters.of("foo", instant), variable.bind(instant).toLowParameter());
    }

    @Test
    void testOfZonedDateTime() {
        var variable = TgVariable.ofZonedDateTime("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.INSTANT, variable.type());

        var zone = ZoneId.of("Asia/Tokyo");
        var dateTime = ZonedDateTime.of(2022, 6, 2, 23, 30, 59, 999, zone);
        assertEquals(Parameters.of("foo", dateTime.toInstant()), variable.bind(dateTime).toLowParameter());
    }

    @Test
    void testSqlName() {
        var variable = TgVariable.ofInt4("foo");
        assertEquals(":foo", variable.sqlName());
    }

    @Test
    void testToString() {
        var variable = TgVariable.ofInt4("foo");
        assertEquals("TgVariable{name=foo, type=INT4}", variable.toString());
    }
}
