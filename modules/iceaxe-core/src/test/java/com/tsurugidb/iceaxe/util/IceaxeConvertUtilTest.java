package com.tsurugidb.iceaxe.util;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.junit.jupiter.api.Test;

class IceaxeConvertUtilTest {

    @Test
    void testToBoolean() {
        assertNull(IceaxeConvertUtil.toBoolean(null));
        assertEquals(true, IceaxeConvertUtil.toBoolean(true));
        assertEquals(false, IceaxeConvertUtil.toBoolean(false));
        assertEquals(true, IceaxeConvertUtil.toBoolean((byte) 1));
        assertEquals(false, IceaxeConvertUtil.toBoolean((byte) 0));
        assertEquals(true, IceaxeConvertUtil.toBoolean((short) 1));
        assertEquals(false, IceaxeConvertUtil.toBoolean((short) 0));
        assertEquals(true, IceaxeConvertUtil.toBoolean(1));
        assertEquals(false, IceaxeConvertUtil.toBoolean(0));
        assertEquals(true, IceaxeConvertUtil.toBoolean(1L));
        assertEquals(false, IceaxeConvertUtil.toBoolean(0L));
        assertEquals(true, IceaxeConvertUtil.toBoolean(1f));
        assertEquals(false, IceaxeConvertUtil.toBoolean(0f));
        assertEquals(true, IceaxeConvertUtil.toBoolean(1d));
        assertEquals(false, IceaxeConvertUtil.toBoolean(0d));
        assertEquals(true, IceaxeConvertUtil.toBoolean(new BigDecimal(1)));
        assertEquals(false, IceaxeConvertUtil.toBoolean(new BigDecimal(0)));
        assertEquals(true, IceaxeConvertUtil.toBoolean(new BigInteger("1")));
        assertEquals(false, IceaxeConvertUtil.toBoolean(new BigInteger("0")));
        assertEquals(true, IceaxeConvertUtil.toBoolean("true"));
        assertEquals(false, IceaxeConvertUtil.toBoolean("false"));
    }

    @Test
    void testToInt4() {
        assertNull(IceaxeConvertUtil.toInt4(null));
        assertEquals(Integer.valueOf(123), IceaxeConvertUtil.toInt4((byte) 123));
        assertEquals(Integer.valueOf(123), IceaxeConvertUtil.toInt4((short) 123));
        assertEquals(Integer.valueOf(123), IceaxeConvertUtil.toInt4(123));
        assertEquals(Integer.valueOf(123), IceaxeConvertUtil.toInt4(123L));
        assertEquals(Integer.valueOf(123), IceaxeConvertUtil.toInt4(123f));
        assertEquals(Integer.valueOf(123), IceaxeConvertUtil.toInt4(123d));
        assertEquals(Integer.valueOf(123), IceaxeConvertUtil.toInt4(BigDecimal.valueOf(123)));
        assertEquals(Integer.valueOf(123), IceaxeConvertUtil.toInt4("123"));
    }

    @Test
    void testToInt8() {
        assertNull(IceaxeConvertUtil.toInt8(null));
        assertEquals(Long.valueOf(123), IceaxeConvertUtil.toInt8((byte) 123));
        assertEquals(Long.valueOf(123), IceaxeConvertUtil.toInt8((short) 123));
        assertEquals(Long.valueOf(123), IceaxeConvertUtil.toInt8(123));
        assertEquals(Long.valueOf(123), IceaxeConvertUtil.toInt8(123L));
        assertEquals(Long.valueOf(123), IceaxeConvertUtil.toInt8(123f));
        assertEquals(Long.valueOf(123), IceaxeConvertUtil.toInt8(123d));
        assertEquals(Long.valueOf(123), IceaxeConvertUtil.toInt8(BigDecimal.valueOf(123)));
        assertEquals(Long.valueOf(123), IceaxeConvertUtil.toInt8("123"));
    }

    @Test
    void testToFloat4() {
        assertNull(IceaxeConvertUtil.toFloat4(null));
        assertEquals(Float.valueOf(123), IceaxeConvertUtil.toFloat4(123));
        assertEquals(Float.valueOf(123), IceaxeConvertUtil.toFloat4(123L));
        assertEquals(Float.valueOf(123), IceaxeConvertUtil.toFloat4(123f));
        assertEquals(Float.valueOf(123), IceaxeConvertUtil.toFloat4(123d));
        assertEquals(Float.valueOf(123), IceaxeConvertUtil.toFloat4(BigDecimal.valueOf(123)));
        assertEquals(Float.valueOf(123), IceaxeConvertUtil.toFloat4("123"));
    }

    @Test
    void testToFloat8() {
        assertNull(IceaxeConvertUtil.toFloat8(null));
        assertEquals(Double.valueOf(123), IceaxeConvertUtil.toFloat8(123));
        assertEquals(Double.valueOf(123), IceaxeConvertUtil.toFloat8(123L));
        assertEquals(Double.valueOf(123), IceaxeConvertUtil.toFloat8(123f));
        assertEquals(Double.valueOf(123), IceaxeConvertUtil.toFloat8(123d));
        assertEquals(Double.valueOf(123), IceaxeConvertUtil.toFloat8(BigDecimal.valueOf(123)));
        assertEquals(Double.valueOf(123), IceaxeConvertUtil.toFloat8("123"));
    }

    @Test
    void testToDecimal() {
        assertNull(IceaxeConvertUtil.toDecimal(null));
        assertEquals(BigDecimal.valueOf(123), IceaxeConvertUtil.toDecimal((byte) 123));
        assertEquals(BigDecimal.valueOf(123), IceaxeConvertUtil.toDecimal((short) 123));
        assertEquals(BigDecimal.valueOf(123), IceaxeConvertUtil.toDecimal(123));
        assertEquals(BigDecimal.valueOf(123), IceaxeConvertUtil.toDecimal(123L));
        assertEquals(BigDecimal.valueOf(123.0), IceaxeConvertUtil.toDecimal(123f));
        assertEquals(BigDecimal.valueOf(123.0), IceaxeConvertUtil.toDecimal(123d));
        assertEquals(BigDecimal.valueOf(123), IceaxeConvertUtil.toDecimal(new BigDecimal("123")));
        assertEquals(BigDecimal.valueOf(123), IceaxeConvertUtil.toDecimal(new BigInteger("123")));
        assertEquals(BigDecimal.valueOf(123), IceaxeConvertUtil.toDecimal("123"));
    }

    @Test
    void testToCharacter() {
        assertNull(IceaxeConvertUtil.toCharacter(null));
        assertEquals("123", IceaxeConvertUtil.toCharacter((byte) 123));
        assertEquals("123", IceaxeConvertUtil.toCharacter((short) 123));
        assertEquals("123", IceaxeConvertUtil.toCharacter(123));
        assertEquals("123", IceaxeConvertUtil.toCharacter(123L));
        assertEquals("123.0", IceaxeConvertUtil.toCharacter(123f));
        assertEquals("123.0", IceaxeConvertUtil.toCharacter(123d));
        assertEquals("123", IceaxeConvertUtil.toCharacter(BigDecimal.valueOf(123)));
        assertEquals("123", IceaxeConvertUtil.toCharacter("123"));
    }

    @Test
    void testToBytes() {
        assertNull(IceaxeConvertUtil.toBytes(null));
        assertArrayEquals(new byte[] {}, IceaxeConvertUtil.toBytes(new byte[] {}));
        assertArrayEquals(new byte[] { 1, 2, 3 }, IceaxeConvertUtil.toBytes(new byte[] { 1, 2, 3 }));
    }

    @Test
    void testToBits() {
        assertNull(IceaxeConvertUtil.toBits(null));
        assertArrayEquals(new boolean[] {}, IceaxeConvertUtil.toBits(new boolean[] {}));
        assertArrayEquals(new boolean[] { true, false }, IceaxeConvertUtil.toBits(new boolean[] { true, false }));
    }

    @Test
    void testToDate() {
        assertNull(IceaxeConvertUtil.toDate(null));
        var expected = LocalDate.of(2022, 6, 2);
        assertEquals(expected, IceaxeConvertUtil.toDate(LocalDate.of(2022, 6, 2)));
        assertEquals(expected, IceaxeConvertUtil.toDate(LocalDateTime.of(2022, 6, 2, 23, 59)));
        assertEquals(expected, IceaxeConvertUtil.toDate(java.sql.Date.valueOf("2022-06-02")));
    }

    @Test
    void testToTime() {
        assertNull(IceaxeConvertUtil.toTime(null));
        var expected = LocalTime.of(23, 30, 59);
        assertEquals(expected, IceaxeConvertUtil.toTime(LocalTime.of(23, 30, 59)));
        assertEquals(expected, IceaxeConvertUtil.toTime(LocalDateTime.of(2022, 6, 2, 23, 30, 59)));
        assertEquals(expected, IceaxeConvertUtil.toTime(java.sql.Time.valueOf("23:30:59")));
    }

    @Test
    void testToInstant() {
        assertNull(IceaxeConvertUtil.toInstant(null));
        var zone = ZoneId.of("Asia/Tokyo");
        var expected = ZonedDateTime.of(2022, 6, 2, 23, 30, 59, 999, zone).toInstant();
        assertEquals(expected, IceaxeConvertUtil.toInstant(ZonedDateTime.of(2022, 6, 2, 23, 30, 59, 999, zone).toInstant()));
        assertEquals(expected, IceaxeConvertUtil.toInstant(ZonedDateTime.of(2022, 6, 2, 23, 30, 59, 999, zone)));
        assertEquals(expected, IceaxeConvertUtil.toInstant(OffsetDateTime.of(2022, 6, 2, 23, 30, 59, 999, ZoneOffset.ofHours(+9))));
    }

    @Test
    void testToZonedDateTime() {
        var zone = ZoneId.of("Asia/Tokyo");
        assertNull(IceaxeConvertUtil.toZonedDateTime(null, zone));
        var expected = ZonedDateTime.of(2022, 6, 2, 23, 30, 59, 999, zone);
        assertEquals(expected, IceaxeConvertUtil.toZonedDateTime(ZonedDateTime.of(2022, 6, 2, 23, 30, 59, 999, zone).toInstant(), zone));
        assertEquals(expected, IceaxeConvertUtil.toZonedDateTime(ZonedDateTime.of(2022, 6, 2, 23, 30, 59, 999, zone), zone));
        assertEquals(expected, IceaxeConvertUtil.toZonedDateTime(OffsetDateTime.of(2022, 6, 2, 23, 30, 59, 999, ZoneOffset.ofHours(+9)), zone));
    }
}
