package com.tsurugidb.iceaxe.statement;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.low.sql.Parameters;

class TgParameterTest {

    @Test
    void testOfStringBoolean() {
        assertEquals(Parameters.ofNull("foo"), TgParameter.of("foo", (Boolean) null).toLowParameter());

        var parameter = TgParameter.of("foo", true);
        assertEquals(Parameters.of("foo", true), parameter.toLowParameter());
    }

    @Test
    void testOfStringInteger() {
        assertEquals(Parameters.ofNull("foo"), TgParameter.of("foo", (Integer) null).toLowParameter());

        var parameter = TgParameter.of("foo", 123);
        assertEquals(Parameters.of("foo", 123), parameter.toLowParameter());
    }

    @Test
    void testOfStringLong() {
        assertEquals(Parameters.ofNull("foo"), TgParameter.of("foo", (Long) null).toLowParameter());

        var parameter = TgParameter.of("foo", 123L);
        assertEquals(Parameters.of("foo", 123L), parameter.toLowParameter());
    }

    @Test
    void testOfStringFloat() {
        assertEquals(Parameters.ofNull("foo"), TgParameter.of("foo", (Float) null).toLowParameter());

        var parameter = TgParameter.of("foo", 123f);
        assertEquals(Parameters.of("foo", 123f), parameter.toLowParameter());
    }

    @Test
    void testOfStringDouble() {
        assertEquals(Parameters.ofNull("foo"), TgParameter.of("foo", (Double) null).toLowParameter());

        var parameter = TgParameter.of("foo", 123d);
        assertEquals(Parameters.of("foo", 123d), parameter.toLowParameter());
    }

    @Test
    void testOfStringBigDecimal() {
        assertEquals(Parameters.ofNull("foo"), TgParameter.of("foo", (BigDecimal) null).toLowParameter());

        var parameter = TgParameter.of("foo", BigDecimal.valueOf(123));
        assertEquals(Parameters.of("foo", BigDecimal.valueOf(123)), parameter.toLowParameter());
    }

    @Test
    void testOfStringString() {
        assertEquals(Parameters.ofNull("foo"), TgParameter.of("foo", (String) null).toLowParameter());

        var parameter = TgParameter.of("foo", "abc");
        assertEquals(Parameters.of("foo", "abc"), parameter.toLowParameter());
    }

    @Test
    void testOfStringByteArray() {
        assertEquals(Parameters.ofNull("foo"), TgParameter.of("foo", (byte[]) null).toLowParameter());

        var parameter = TgParameter.of("foo", new byte[] { 1, 2, 3 });
        assertEquals(Parameters.of("foo", new byte[] { 1, 2, 3 }), parameter.toLowParameter());
    }

    @Test
    void testOfStringBooleanArray() {
        assertEquals(Parameters.ofNull("foo"), TgParameter.of("foo", (boolean[]) null).toLowParameter());

        var parameter = TgParameter.of("foo", new boolean[] { true, false, true });
        assertEquals(Parameters.of("foo", new boolean[] { true, false, true }), parameter.toLowParameter());
    }

    @Test
    void testOfStringLocalDate() {
        assertEquals(Parameters.ofNull("foo"), TgParameter.of("foo", (LocalDate) null).toLowParameter());

        var parameter = TgParameter.of("foo", LocalDate.of(2022, 6, 3));
        assertEquals(Parameters.of("foo", LocalDate.of(2022, 6, 3)), parameter.toLowParameter());
    }

    @Test
    void testOfStringLocalTime() {
        assertEquals(Parameters.ofNull("foo"), TgParameter.of("foo", (LocalTime) null).toLowParameter());

        var parameter = TgParameter.of("foo", LocalTime.of(23, 30, 59));
        assertEquals(Parameters.of("foo", LocalTime.of(23, 30, 59)), parameter.toLowParameter());
    }

    @Test
    void testOfStringInstant() {
        assertEquals(Parameters.ofNull("foo"), TgParameter.of("foo", (Instant) null).toLowParameter());

        var zone = ZoneId.of("Asia/Tokyo");
        var instant = ZonedDateTime.of(2022, 6, 3, 23, 30, 59, 999, zone).toInstant();
        var parameter = TgParameter.of("foo", instant);
        assertEquals(Parameters.of("foo", instant), parameter.toLowParameter());
    }

    @Test
    void testOfStringZonedDateTime() {
        assertEquals(Parameters.ofNull("foo"), TgParameter.of("foo", (ZonedDateTime) null).toLowParameter());

        var zone = ZoneId.of("Asia/Tokyo");
        var dateTime = ZonedDateTime.of(2022, 6, 3, 23, 30, 59, 999, zone);
        var parameter = TgParameter.of("foo", dateTime);
        assertEquals(Parameters.of("foo", dateTime.toInstant()), parameter.toLowParameter());
    }
}
