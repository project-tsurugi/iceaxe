package com.tsurugidb.iceaxe.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.sql.Parameters;

class IceaxeLowParameterUtilTest {

    @Test
    void testCreateStringBoolean() {
        var parameter = IceaxeLowParameterUtil.create("foo", true);
        assertEquals(Parameters.of("foo", true), parameter);
    }

    @Test
    void testCreateStringBooleanWrapper() {
        assertEquals(Parameters.ofNull("foo"), IceaxeLowParameterUtil.create("foo", (Boolean) null));

        var parameter = IceaxeLowParameterUtil.create("foo", Boolean.TRUE);
        assertEquals(Parameters.of("foo", true), parameter);
    }

    @Test
    void testCreateStringInt() {
        var parameter = IceaxeLowParameterUtil.create("foo", 123);
        assertEquals(Parameters.of("foo", 123), parameter);
    }

    @Test
    void testCreateStringInteger() {
        assertEquals(Parameters.ofNull("foo"), IceaxeLowParameterUtil.create("foo", (Integer) null));

        var parameter = IceaxeLowParameterUtil.create("foo", Integer.valueOf(123));
        assertEquals(Parameters.of("foo", 123), parameter);
    }

    @Test
    void testCreateStringLong() {
        var parameter = IceaxeLowParameterUtil.create("foo", 123L);
        assertEquals(Parameters.of("foo", 123L), parameter);
    }

    @Test
    void testCreateStringLongWrapper() {
        assertEquals(Parameters.ofNull("foo"), IceaxeLowParameterUtil.create("foo", (Long) null));

        var parameter = IceaxeLowParameterUtil.create("foo", Long.valueOf(123));
        assertEquals(Parameters.of("foo", 123L), parameter);
    }

    @Test
    void testCreateStringFloat() {
        var parameter = IceaxeLowParameterUtil.create("foo", 123f);
        assertEquals(Parameters.of("foo", 123f), parameter);
    }

    @Test
    void testCreateStringFloatWrapper() {
        assertEquals(Parameters.ofNull("foo"), IceaxeLowParameterUtil.create("foo", (Float) null));

        var parameter = IceaxeLowParameterUtil.create("foo", Float.valueOf(123));
        assertEquals(Parameters.of("foo", 123f), parameter);
    }

    @Test
    void testCreateStringDouble() {
        var parameter = IceaxeLowParameterUtil.create("foo", 123d);
        assertEquals(Parameters.of("foo", 123d), parameter);
    }

    @Test
    void testCreateStringDoubleWrapper() {
        assertEquals(Parameters.ofNull("foo"), IceaxeLowParameterUtil.create("foo", (Double) null));

        var parameter = IceaxeLowParameterUtil.create("foo", Double.valueOf(123));
        assertEquals(Parameters.of("foo", 123d), parameter);
    }

    @Test
    void testCreateStringBigDecimal() {
        assertEquals(Parameters.ofNull("foo"), IceaxeLowParameterUtil.create("foo", (BigDecimal) null));

        var parameter = IceaxeLowParameterUtil.create("foo", BigDecimal.valueOf(123));
        assertEquals(Parameters.of("foo", BigDecimal.valueOf(123)), parameter);
    }

    @Test
    void testCreateStringString() {
        assertEquals(Parameters.ofNull("foo"), IceaxeLowParameterUtil.create("foo", (String) null));

        var parameter = IceaxeLowParameterUtil.create("foo", "abc");
        assertEquals(Parameters.of("foo", "abc"), parameter);
    }

    @Test
    void testCreateStringByteArray() {
        assertEquals(Parameters.ofNull("foo"), IceaxeLowParameterUtil.create("foo", (byte[]) null));

        var parameter = IceaxeLowParameterUtil.create("foo", new byte[] { 1, 2, 3 });
        assertEquals(Parameters.of("foo", new byte[] { 1, 2, 3 }), parameter);
    }

    @Test
    void testCreateStringBooleanArray() {
        assertEquals(Parameters.ofNull("foo"), IceaxeLowParameterUtil.create("foo", (boolean[]) null));

        var parameter = IceaxeLowParameterUtil.create("foo", new boolean[] { true, false, true });
        assertEquals(Parameters.of("foo", new boolean[] { true, false, true }), parameter);
    }

    @Test
    void testCreateStringLocalDate() {
        assertEquals(Parameters.ofNull("foo"), IceaxeLowParameterUtil.create("foo", (LocalDate) null));

        var parameter = IceaxeLowParameterUtil.create("foo", LocalDate.of(2022, 6, 3));
        assertEquals(Parameters.of("foo", LocalDate.of(2022, 6, 3)), parameter);
    }

    @Test
    void testCreateStringLocalTime() {
        assertEquals(Parameters.ofNull("foo"), IceaxeLowParameterUtil.create("foo", (LocalTime) null));

        var parameter = IceaxeLowParameterUtil.create("foo", LocalTime.of(23, 30, 59));
        assertEquals(Parameters.of("foo", LocalTime.of(23, 30, 59)), parameter);
    }

    @Test
    void testCreateStringInstant() {
        assertEquals(Parameters.ofNull("foo"), IceaxeLowParameterUtil.create("foo", (Instant) null));

        var zone = ZoneId.of("Asia/Tokyo");
        var instant = ZonedDateTime.of(2022, 6, 3, 23, 30, 59, 999, zone).toInstant();
        var parameter = IceaxeLowParameterUtil.create("foo", instant);
        assertEquals(Parameters.of("foo", instant), parameter);
    }

    @Test
    void testCreateStringZonedDateTime() {
        assertEquals(Parameters.ofNull("foo"), IceaxeLowParameterUtil.create("foo", (ZonedDateTime) null));

        var zone = ZoneId.of("Asia/Tokyo");
        var dateTime = ZonedDateTime.of(2022, 6, 3, 23, 30, 59, 999, zone);
        var parameter = IceaxeLowParameterUtil.create("foo", dateTime);
        assertEquals(Parameters.of("foo", dateTime.toInstant()), parameter);
    }
}
