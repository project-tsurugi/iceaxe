package com.tsurugidb.iceaxe.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;

import org.junit.jupiter.api.Test;

class IceaxeConvertUtilTest {

    private final IceaxeConvertUtil target = IceaxeConvertUtil.INSTANCE;

    @Test
    void testToBoolean() {
        assertNull(target.toBoolean(null));

        assertEquals(true, target.toBoolean(true));
        assertEquals(false, target.toBoolean(false));
        assertEquals(true, target.toBoolean((byte) 1));
        assertEquals(false, target.toBoolean((byte) 0));
        assertEquals(true, target.toBoolean((short) 1));
        assertEquals(false, target.toBoolean((short) 0));
        assertEquals(true, target.toBoolean(1));
        assertEquals(false, target.toBoolean(0));
        assertEquals(true, target.toBoolean(1L));
        assertEquals(false, target.toBoolean(0L));
        assertEquals(true, target.toBoolean(1f));
        assertEquals(false, target.toBoolean(0f));
        assertEquals(true, target.toBoolean(1d));
        assertEquals(false, target.toBoolean(0d));
        assertEquals(true, target.toBoolean(new BigDecimal(1)));
        assertEquals(false, target.toBoolean(new BigDecimal(0)));
        assertEquals(true, target.toBoolean(new BigInteger("1")));
        assertEquals(false, target.toBoolean(new BigInteger("0")));
        assertEquals(true, target.toBoolean("true"));
        assertEquals(false, target.toBoolean("false"));

        var e = assertThrows(UnsupportedOperationException.class, () -> new IceaxeConvertUtil() {
            @Override
            protected Boolean convertBoolean(Object obj) {
                throw new RuntimeException("test");
            }
        }.toBoolean("true"));
        assertEquals("test", e.getCause().getMessage());
    }

    @Test
    void testToInt4() {
        assertNull(target.toInt4(null));

        assertEquals(Integer.valueOf(123), target.toInt4((byte) 123));
        assertEquals(Integer.valueOf(123), target.toInt4((short) 123));
        assertEquals(Integer.valueOf(123), target.toInt4(123));
        assertEquals(Integer.valueOf(123), target.toInt4(123L));
        assertEquals(Integer.valueOf(123), target.toInt4(123f));
        assertEquals(Integer.valueOf(123), target.toInt4(123d));
        assertEquals(Integer.valueOf(123), target.toInt4(BigDecimal.valueOf(123)));
        assertEquals(Integer.valueOf(123), target.toInt4("123"));

        var e = assertThrows(UnsupportedOperationException.class, () -> target.toInt4("abc"));
        assertInstanceOf(NumberFormatException.class, e.getCause());
    }

    @Test
    void testToInt8() {
        assertNull(target.toInt8(null));

        assertEquals(Long.valueOf(123), target.toInt8((byte) 123));
        assertEquals(Long.valueOf(123), target.toInt8((short) 123));
        assertEquals(Long.valueOf(123), target.toInt8(123));
        assertEquals(Long.valueOf(123), target.toInt8(123L));
        assertEquals(Long.valueOf(123), target.toInt8(123f));
        assertEquals(Long.valueOf(123), target.toInt8(123d));
        assertEquals(Long.valueOf(123), target.toInt8(BigDecimal.valueOf(123)));
        assertEquals(Long.valueOf(123), target.toInt8("123"));

        var e = assertThrows(UnsupportedOperationException.class, () -> target.toInt8("abc"));
        assertInstanceOf(NumberFormatException.class, e.getCause());
    }

    @Test
    void testToFloat4() {
        assertNull(target.toFloat4(null));

        assertEquals(Float.valueOf(123), target.toFloat4(123));
        assertEquals(Float.valueOf(123), target.toFloat4(123L));
        assertEquals(Float.valueOf(123), target.toFloat4(123f));
        assertEquals(Float.valueOf(123), target.toFloat4(123d));
        assertEquals(Float.valueOf(123), target.toFloat4(BigDecimal.valueOf(123)));
        assertEquals(Float.valueOf(123), target.toFloat4("123"));

        var e = assertThrows(UnsupportedOperationException.class, () -> target.toFloat4("abc"));
        assertInstanceOf(NumberFormatException.class, e.getCause());
    }

    @Test
    void testToFloat8() {
        assertNull(target.toFloat8(null));

        assertEquals(Double.valueOf(123), target.toFloat8(123));
        assertEquals(Double.valueOf(123), target.toFloat8(123L));
        assertEquals(Double.valueOf(123), target.toFloat8(123f));
        assertEquals(Double.valueOf(123), target.toFloat8(123d));
        assertEquals(Double.valueOf(123), target.toFloat8(BigDecimal.valueOf(123)));
        assertEquals(Double.valueOf(123), target.toFloat8("123"));

        var e = assertThrows(UnsupportedOperationException.class, () -> target.toFloat8("abc"));
        assertInstanceOf(NumberFormatException.class, e.getCause());
    }

    @Test
    void testToDecimal() {
        assertNull(target.toDecimal(null));

        assertEquals(BigDecimal.valueOf(123), target.toDecimal((byte) 123));
        assertEquals(BigDecimal.valueOf(123), target.toDecimal((short) 123));
        assertEquals(BigDecimal.valueOf(123), target.toDecimal(123));
        assertEquals(BigDecimal.valueOf(123), target.toDecimal(123L));
        assertEquals(BigDecimal.valueOf(123.0), target.toDecimal(123f));
        assertEquals(BigDecimal.valueOf(123.0), target.toDecimal(123d));
        assertEquals(BigDecimal.valueOf(123), target.toDecimal(new BigDecimal("123")));
        assertEquals(BigDecimal.valueOf(123), target.toDecimal(new BigInteger("123")));
        assertEquals(BigDecimal.valueOf(123), target.toDecimal("123"));

        var e = assertThrows(UnsupportedOperationException.class, () -> target.toDecimal("abc"));
        assertInstanceOf(NumberFormatException.class, e.getCause());
    }

    @Test
    void testToCharacter() {
        assertNull(target.toCharacter(null));

        assertEquals("123", target.toCharacter((byte) 123));
        assertEquals("123", target.toCharacter((short) 123));
        assertEquals("123", target.toCharacter(123));
        assertEquals("123", target.toCharacter(123L));
        assertEquals("123.0", target.toCharacter(123f));
        assertEquals("123.0", target.toCharacter(123d));
        assertEquals("123", target.toCharacter(BigDecimal.valueOf(123)));
        assertEquals("123", target.toCharacter("123"));

        var e = assertThrows(UnsupportedOperationException.class, () -> new IceaxeConvertUtil() {
            @Override
            protected String convertString(Object obj) {
                throw new RuntimeException("test");
            }
        }.toCharacter("123"));
        assertEquals("test", e.getCause().getMessage());
    }

    @Test
    void testToBytes() {
        assertNull(target.toBytes(null));

        assertArrayEquals(new byte[] {}, target.toBytes(new byte[] {}));
        assertArrayEquals(new byte[] { 1, 2, 3 }, target.toBytes(new byte[] { 1, 2, 3 }));

        var e = assertThrows(UnsupportedOperationException.class, () -> new IceaxeConvertUtil() {
            @Override
            protected byte[] convertBytes(Object obj) {
                throw new RuntimeException("test");
            }
        }.toBytes("123"));
        assertEquals("test", e.getCause().getMessage());
    }

    @Test
    void testToBits() {
        assertNull(target.toBits(null));

        assertArrayEquals(new boolean[] {}, target.toBits(new boolean[] {}));
        assertArrayEquals(new boolean[] { true, false }, target.toBits(new boolean[] { true, false }));

        var e = assertThrows(UnsupportedOperationException.class, () -> new IceaxeConvertUtil() {
            @Override
            protected boolean[] convertBits(Object obj) {
                throw new RuntimeException("test");
            }
        }.toBits("123"));
        assertEquals("test", e.getCause().getMessage());
    }

    @Test
    void testToDate() {
        assertNull(target.toDate(null));

        var expected = LocalDate.of(2022, 6, 2);
        assertEquals(expected, target.toDate(LocalDate.of(2022, 6, 2)));
        assertEquals(expected, target.toDate(LocalDateTime.of(2022, 6, 2, 23, 59)));
        assertEquals(expected, target.toDate(java.sql.Date.valueOf("2022-06-02")));
        assertEquals(expected, target.toDate("2022-06-02"));

        var e = assertThrows(UnsupportedOperationException.class, () -> target.toDate("2022/06/02"));
        assertInstanceOf(DateTimeParseException.class, e.getCause());
    }

    @Test
    void testToTime() {
        assertNull(target.toTime(null));

        var expected = LocalTime.of(23, 30, 59);
        assertEquals(expected, target.toTime(LocalTime.of(23, 30, 59)));
        assertEquals(expected, target.toTime(LocalDateTime.of(2022, 6, 2, 23, 30, 59)));
        assertEquals(expected, target.toTime(java.sql.Time.valueOf("23:30:59")));
        assertEquals(expected, target.toTime("23:30:59"));

        var e = assertThrows(UnsupportedOperationException.class, () -> target.toTime("23;30;59"));
        assertInstanceOf(DateTimeParseException.class, e.getCause());
    }

    @Test
    void testToDateTime() {
        assertNull(target.toDateTime(null));

        var expected = LocalDateTime.of(2022, 9, 22, 23, 30, 59);
        assertEquals(expected, target.toDateTime(LocalDateTime.of(2022, 9, 22, 23, 30, 59)));
        assertEquals(expected, target.toDateTime(ZonedDateTime.of(2022, 9, 22, 23, 30, 59, 0, ZoneId.of("Asia/Tokyo"))));
        assertEquals(expected, target.toDateTime(OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, ZoneOffset.ofHours(9))));
        assertEquals(expected, target.toDateTime(java.sql.Timestamp.valueOf("2022-09-22 23:30:59")));
        assertEquals(expected, target.toDateTime("2022-09-22T23:30:59"));

        var e = assertThrows(UnsupportedOperationException.class, () -> target.toDateTime("2022-09-22 23:30:59"));
        assertInstanceOf(DateTimeParseException.class, e.getCause());
    }

    @Test
    void testToOffsetTime() {
        assertNull(target.toOffsetTime(null));

        var offset = ZoneOffset.ofHours(9);
        var expected = OffsetTime.of(23, 30, 59, 0, offset);
        assertEquals(expected, target.toOffsetTime(OffsetTime.of(23, 30, 59, 0, offset)));
        assertEquals(expected, target.toOffsetTime(ZonedDateTime.of(2022, 9, 22, 23, 30, 59, 0, ZoneId.of("Asia/Tokyo"))));
        assertEquals(expected, target.toOffsetTime(OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset)));
        assertEquals(expected, target.toOffsetTime("23:30:59+09:00"));

        var e = assertThrows(UnsupportedOperationException.class, () -> target.toOffsetTime("23;30;59"));
        assertInstanceOf(DateTimeParseException.class, e.getCause());
    }

    @Test
    void testToOffsetDateTime() {
        assertNull(target.toOffsetDateTime(null));

        var offset = ZoneOffset.ofHours(9);
        var expected = OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset);
        assertEquals(expected, target.toOffsetDateTime(OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset)));
        assertEquals(expected, target.toOffsetDateTime(ZonedDateTime.of(2022, 9, 22, 23, 30, 59, 0, ZoneId.of("Asia/Tokyo"))));
        assertEquals(expected, target.toOffsetDateTime("2022-09-22T23:30:59+09:00"));

        var e = assertThrows(UnsupportedOperationException.class, () -> target.toOffsetDateTime("2022-09-22 23:30:59"));
        assertInstanceOf(DateTimeParseException.class, e.getCause());
    }

    @Test
    void testToZonedDateTime() {
        var zone = ZoneId.of("Asia/Tokyo");
        assertNull(target.toZonedDateTime(null, zone));

        var expected = ZonedDateTime.of(2022, 6, 2, 23, 30, 59, 999, zone);
        assertEquals(expected, target.toZonedDateTime(ZonedDateTime.of(2022, 6, 2, 23, 30, 59, 999, zone).toInstant(), zone));
        assertEquals(expected, target.toZonedDateTime(ZonedDateTime.of(2022, 6, 2, 23, 30, 59, 999, zone), zone));
        assertEquals(expected, target.toZonedDateTime(OffsetDateTime.of(2022, 6, 2, 23, 30, 59, 999, ZoneOffset.ofHours(9)), zone));

        var e = assertThrows(UnsupportedOperationException.class, () -> new IceaxeConvertUtil() {
            @Override
            protected ZonedDateTime convertZonedDateTime(Object obj, ZoneId zone) {
                throw new RuntimeException("test");
            }
        }.toZonedDateTime("123", zone));
        assertEquals("test", e.getCause().getMessage());
    }
}
