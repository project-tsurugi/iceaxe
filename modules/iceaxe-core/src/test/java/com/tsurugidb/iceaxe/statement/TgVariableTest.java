package com.tsurugidb.iceaxe.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.sql.Parameters;

class TgVariableTest {

    @Test
    void testOfBoolean() {
        var variable = TgVariable.ofBoolean("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.BOOLEAN, variable.type());
        assertEquals(Parameters.of("foo", true), variable.bind(true).toLowParameter());
        assertEquals(Parameters.of("foo", true), variable.bind(Boolean.TRUE).toLowParameter());

        var copy = variable.copy("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());
    }

    @Test
    void testOfInt4() {
        var variable = TgVariable.ofInt4("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.INT4, variable.type());
        assertEquals(Parameters.of("foo", 123), variable.bind(123).toLowParameter());
        assertEquals(Parameters.of("foo", 123), variable.bind(Integer.valueOf(123)).toLowParameter());

        var copy = variable.copy("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());
    }

    @Test
    void testOfInt8() {
        var variable = TgVariable.ofInt8("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.INT8, variable.type());
        assertEquals(Parameters.of("foo", 123L), variable.bind(123).toLowParameter());
        assertEquals(Parameters.of("foo", 123L), variable.bind(Long.valueOf(123)).toLowParameter());

        var copy = variable.copy("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());
    }

    @Test
    void testOfFloat4() {
        var variable = TgVariable.ofFloat4("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.FLOAT4, variable.type());
        assertEquals(Parameters.of("foo", 123f), variable.bind(123).toLowParameter());
        assertEquals(Parameters.of("foo", 123f), variable.bind(Float.valueOf(123)).toLowParameter());

        var copy = variable.copy("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());
    }

    @Test
    void testOfFloat8() {
        var variable = TgVariable.ofFloat8("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.FLOAT8, variable.type());
        assertEquals(Parameters.of("foo", 123d), variable.bind(123).toLowParameter());
        assertEquals(Parameters.of("foo", 123d), variable.bind(Double.valueOf(123)).toLowParameter());

        var copy = variable.copy("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());
    }

    @Test
    void testOfDecimal() {
        var variable = TgVariable.ofDecimal("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.DECIMAL, variable.type());
        assertEquals(Parameters.of("foo", BigDecimal.valueOf(123)), variable.bind(BigDecimal.valueOf(123)).toLowParameter());

        var copy = variable.copy("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());
    }

    @Test
    void testOfCharacter() {
        var variable = TgVariable.ofCharacter("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.CHARACTER, variable.type());
        assertEquals(Parameters.of("foo", "abc"), variable.bind("abc").toLowParameter());

        var copy = variable.copy("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());
    }

    @Test
    void testOfBytes() {
        var variable = TgVariable.ofBytes("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.BYTES, variable.type());
        assertEquals(Parameters.of("foo", new byte[] { 1, 2, 3 }), variable.bind(new byte[] { 1, 2, 3 }).toLowParameter());

        var copy = variable.copy("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());
    }

    @Test
    void testOfBits() {
        var variable = TgVariable.ofBits("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.BITS, variable.type());
        assertEquals(Parameters.of("foo", new boolean[] { true, false, true }), variable.bind(new boolean[] { true, false, true }).toLowParameter());

        var copy = variable.copy("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());
    }

    @Test
    void testOfDate() {
        var variable = TgVariable.ofDate("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.DATE, variable.type());
        assertEquals(Parameters.of("foo", LocalDate.of(2022, 6, 2)), variable.bind(LocalDate.of(2022, 6, 2)).toLowParameter());

        var copy = variable.copy("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());
    }

    @Test
    void testOfTime() {
        var variable = TgVariable.ofTime("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.TIME, variable.type());
        assertEquals(Parameters.of("foo", LocalTime.of(23, 30, 59)), variable.bind(LocalTime.of(23, 30, 59)).toLowParameter());

        var copy = variable.copy("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());
    }

    @Test
    void testOfDateTime() {
        var variable = TgVariable.ofDateTime("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.DATE_TIME, variable.type());
        assertEquals(Parameters.of("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59)), variable.bind(LocalDateTime.of(2022, 9, 22, 23, 30, 59)).toLowParameter());

        var copy = variable.copy("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());
    }

    @Test
    void testOfOffsetTime() {
        var variable = TgVariable.ofOffsetTime("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.OFFSET_TIME, variable.type());
        var offset = ZoneOffset.ofHours(9);
        assertEquals(Parameters.of("foo", OffsetTime.of(23, 30, 59, 0, offset)), variable.bind(OffsetTime.of(23, 30, 59, 0, offset)).toLowParameter());

        var copy = variable.copy("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());
    }

    @Test
    void testOfOffsetDateTime() {
        var variable = TgVariable.ofOffsetDateTime("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.OFFSET_DATE_TIME, variable.type());
        var offset = ZoneOffset.ofHours(9);
        assertEquals(Parameters.of("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset)), variable.bind(OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset)).toLowParameter());

        var copy = variable.copy("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());
    }

    @Test
    void testOfZonedDateTime() {
        var variable = TgVariable.ofZonedDateTime("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.OFFSET_DATE_TIME, variable.type());

        var zone = ZoneId.of("Asia/Tokyo");
        var dateTime = ZonedDateTime.of(2022, 6, 2, 23, 30, 59, 999, zone);
        assertEquals(Parameters.of("foo", dateTime.toOffsetDateTime()), variable.bind(dateTime).toLowParameter());

        var copy = variable.copy("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());
    }

    @Test
    void testSqlName() {
        var variable = TgVariable.ofInt4("foo");
        assertEquals(":foo", variable.sqlName());
    }

    @Test
    void testToString() {
        var variable = TgVariable.ofInt4("foo");
        assertEquals(":foo/*INT4*/", variable.toString());
    }
}
