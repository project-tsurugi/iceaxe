/*
 * Copyright 2023-2026 Project Tsurugi.
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
package com.tsurugidb.iceaxe.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
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

import com.tsurugidb.iceaxe.sql.type.TgBlob;
import com.tsurugidb.iceaxe.sql.type.TgBlobReference;
import com.tsurugidb.iceaxe.sql.type.TgClob;
import com.tsurugidb.iceaxe.sql.type.TgClobReference;
import com.tsurugidb.iceaxe.test.TestTsurugiTransaction;
import com.tsurugidb.tsubakuro.sql.BlobReference;
import com.tsurugidb.tsubakuro.sql.ClobReference;

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

        var e = assertThrowsExactly(UnsupportedOperationException.class, () -> new IceaxeConvertUtil() {
            @Override
            protected Boolean convertBoolean(Object obj) {
                throw new RuntimeException("test");
            }
        }.toBoolean("true"));
        assertEquals("test", e.getCause().getMessage());
    }

    @Test
    void testToInt() {
        assertNull(target.toInt(null));

        assertEquals(Integer.valueOf(123), target.toInt((byte) 123));
        assertEquals(Integer.valueOf(123), target.toInt((short) 123));
        assertEquals(Integer.valueOf(123), target.toInt(123));
        assertEquals(Integer.valueOf(123), target.toInt(123L));
        assertEquals(Integer.valueOf(123), target.toInt(123f));
        assertEquals(Integer.valueOf(123), target.toInt(123d));
        assertEquals(Integer.valueOf(123), target.toInt(BigDecimal.valueOf(123)));
        assertEquals(Integer.valueOf(123), target.toInt("123"));

        var e = assertThrowsExactly(UnsupportedOperationException.class, () -> target.toInt("abc"));
        assertInstanceOf(NumberFormatException.class, e.getCause());
    }

    @Test
    void testToLong() {
        assertNull(target.toLong(null));

        assertEquals(Long.valueOf(123), target.toLong((byte) 123));
        assertEquals(Long.valueOf(123), target.toLong((short) 123));
        assertEquals(Long.valueOf(123), target.toLong(123));
        assertEquals(Long.valueOf(123), target.toLong(123L));
        assertEquals(Long.valueOf(123), target.toLong(123f));
        assertEquals(Long.valueOf(123), target.toLong(123d));
        assertEquals(Long.valueOf(123), target.toLong(BigDecimal.valueOf(123)));
        assertEquals(Long.valueOf(123), target.toLong("123"));

        var e = assertThrowsExactly(UnsupportedOperationException.class, () -> target.toLong("abc"));
        assertInstanceOf(NumberFormatException.class, e.getCause());
    }

    @Test
    void testToFloat() {
        assertNull(target.toFloat(null));

        assertEquals(Float.valueOf(123), target.toFloat(123));
        assertEquals(Float.valueOf(123), target.toFloat(123L));
        assertEquals(Float.valueOf(123), target.toFloat(123f));
        assertEquals(Float.valueOf(123), target.toFloat(123d));
        assertEquals(Float.valueOf(123), target.toFloat(BigDecimal.valueOf(123)));
        assertEquals(Float.valueOf(123), target.toFloat("123"));

        var e = assertThrowsExactly(UnsupportedOperationException.class, () -> target.toFloat("abc"));
        assertInstanceOf(NumberFormatException.class, e.getCause());
    }

    @Test
    void testToDouble() {
        assertNull(target.toDouble(null));

        assertEquals(Double.valueOf(123), target.toDouble(123));
        assertEquals(Double.valueOf(123), target.toDouble(123L));
        assertEquals(Double.valueOf(123), target.toDouble(123f));
        assertEquals(Double.valueOf(123), target.toDouble(123d));
        assertEquals(Double.valueOf(123), target.toDouble(BigDecimal.valueOf(123)));
        assertEquals(Double.valueOf(123), target.toDouble("123"));

        var e = assertThrowsExactly(UnsupportedOperationException.class, () -> target.toDouble("abc"));
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

        var e = assertThrowsExactly(UnsupportedOperationException.class, () -> target.toDecimal("abc"));
        assertInstanceOf(NumberFormatException.class, e.getCause());
    }

    @Test
    void testToString() {
        assertNull(target.toString(null));

        assertEquals("123", target.toString((byte) 123));
        assertEquals("123", target.toString((short) 123));
        assertEquals("123", target.toString(123));
        assertEquals("123", target.toString(123L));
        assertEquals("123.0", target.toString(123f));
        assertEquals("123.0", target.toString(123d));
        assertEquals("123", target.toString(BigDecimal.valueOf(123)));
        assertEquals("123", target.toString("123"));

        var e = assertThrowsExactly(UnsupportedOperationException.class, () -> new IceaxeConvertUtil() {
            @Override
            protected String convertString(Object obj) {
                throw new RuntimeException("test");
            }
        }.toString("123"));
        assertEquals("test", e.getCause().getMessage());
    }

    @Test
    void testToBytes() {
        assertNull(target.toBytes(null));

        assertArrayEquals(new byte[] {}, target.toBytes(new byte[] {}));
        assertArrayEquals(new byte[] { 1, 2, 3 }, target.toBytes(new byte[] { 1, 2, 3 }));

        var e = assertThrowsExactly(UnsupportedOperationException.class, () -> new IceaxeConvertUtil() {
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

        var e = assertThrowsExactly(UnsupportedOperationException.class, () -> new IceaxeConvertUtil() {
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

        var e = assertThrowsExactly(UnsupportedOperationException.class, () -> target.toDate("2022/06/02"));
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

        var e = assertThrowsExactly(UnsupportedOperationException.class, () -> target.toTime("23;30;59"));
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

        var e = assertThrowsExactly(UnsupportedOperationException.class, () -> target.toDateTime("2022-09-22 23:30:59"));
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

        var e = assertThrowsExactly(UnsupportedOperationException.class, () -> target.toOffsetTime("23;30;59"));
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

        var e = assertThrowsExactly(UnsupportedOperationException.class, () -> target.toOffsetDateTime("2022-09-22 23:30:59"));
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

        var e = assertThrowsExactly(UnsupportedOperationException.class, () -> new IceaxeConvertUtil() {
            @Override
            protected ZonedDateTime convertZonedDateTime(Object obj, ZoneId zone) {
                throw new RuntimeException("test");
            }
        }.toZonedDateTime("123", zone));
        assertEquals("test", e.getCause().getMessage());
    }

    @Test
    void testToBlob() throws Exception {
        assertNull(target.toBlob(null));

        {
            var expected = TgBlob.of(Path.of("/path/to/file"));
            var actual = target.toBlob(expected);
            assertFalse(actual.isDeleteOnExecuteFinished());
            assertSame(expected, actual);
        }
        {
            var expected = Path.of("/path/to/file");
            var actual = target.toBlob(expected);
            assertFalse(actual.isDeleteOnExecuteFinished());
            assertSame(expected, actual.getPath());
        }
        {
            byte[] expected = { 1, 2, 3 };
            try (var actual = target.toBlob(new ByteArrayInputStream(expected))) {
                assertTrue(actual.isDeleteOnExecuteFinished());
                byte[] actualBytes = actual.readAllBytes();
                assertArrayEquals(expected, actualBytes);
            }
        }
        {
            byte[] expected = { 1, 2, 3 };
            try (var actual = target.toBlob(expected)) {
                assertTrue(actual.isDeleteOnExecuteFinished());
                byte[] actualBytes = actual.readAllBytes();
                assertArrayEquals(expected, actualBytes);
            }
        }

        assertThrowsExactly(UnsupportedOperationException.class, () -> target.toBlob("aaa"));
    }

    @Test
    void testToBlobReference() throws Exception {
        assertNull(target.toBlobReference(null));

        {
            var transaction = TestTsurugiTransaction.of();
            var ref = new BlobReference() {
            };
            try (var expected = TgBlobReference.of(transaction, ref)) {
                var actual = target.toBlobReference(expected);
                assertSame(expected, actual);
            }
        }

        assertThrowsExactly(UnsupportedOperationException.class, () -> target.toBlobReference("aaa"));
    }

    @Test
    void testToClob() throws Exception {
        assertNull(target.toClob(null));

        {
            var expected = TgClob.of(Path.of("/path/to/file"));
            var actual = target.toClob(expected);
            assertFalse(actual.isDeleteOnExecuteFinished());
            assertSame(expected, actual);
        }
        {
            var expected = Path.of("/path/to/file");
            var actual = target.toClob(expected);
            assertFalse(actual.isDeleteOnExecuteFinished());
            assertSame(expected, actual.getPath());
        }
        {
            String expected = "abc\ndef\r\nあいうえお";
            try (var actual = target.toClob(new StringReader(expected))) {
                assertTrue(actual.isDeleteOnExecuteFinished());
                String actualString = actual.readString();
                assertEquals(expected, actualString);
            }
        }
        {
            String expected = "abc\ndef\r\nあいうえお";
            try (var actual = target.toClob(expected)) {
                assertTrue(actual.isDeleteOnExecuteFinished());
                String actualString = actual.readString();
                assertEquals(expected, actualString);
            }
        }

        assertThrowsExactly(UnsupportedOperationException.class, () -> target.toClob(new byte[0]));
    }

    @Test
    void testToClobReference() throws Exception {
        assertNull(target.toClobReference(null));

        {
            var transaction = TestTsurugiTransaction.of();
            var ref = new ClobReference() {
            };
            try (var expected = TgClobReference.of(transaction, ref)) {
                var actual = target.toClobReference(expected);
                assertSame(expected, actual);
            }
        }

        assertThrowsExactly(UnsupportedOperationException.class, () -> target.toClobReference("aaa"));
    }
}
