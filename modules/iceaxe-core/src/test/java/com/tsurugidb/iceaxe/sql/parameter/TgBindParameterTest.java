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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.sql.type.TgBlob;
import com.tsurugidb.iceaxe.sql.type.TgClob;
import com.tsurugidb.iceaxe.test.TestBlobUtil;
import com.tsurugidb.iceaxe.test.TestClobUtil;
import com.tsurugidb.iceaxe.util.IceaxeCloseableSet;
import com.tsurugidb.tsubakuro.sql.Parameters;

class TgBindParameterTest {

    @Test
    void testOfStringBoolean() {
        var parameter = TgBindParameter.of("foo", true);
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.of("foo", true), parameter.toLowParameter(closeableSet));
        assertEquals("foo=true(boolean)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfStringBooleanWrapper() {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (Boolean) null).toLowParameter(closeableSet));

        var parameter = TgBindParameter.of("foo", Boolean.TRUE);
        assertEquals(Parameters.of("foo", true), parameter.toLowParameter(closeableSet));
        assertEquals("foo=true(Boolean)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfStringInt() {
        var parameter = TgBindParameter.of("foo", 123);
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.of("foo", 123), parameter.toLowParameter(closeableSet));
        assertEquals("foo=123(int)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfStringInteger() {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (Integer) null).toLowParameter(closeableSet));

        var parameter = TgBindParameter.of("foo", Integer.valueOf(123));
        assertEquals(Parameters.of("foo", 123), parameter.toLowParameter(closeableSet));
        assertEquals("foo=123(Integer)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfStringLong() {
        var parameter = TgBindParameter.of("foo", 123L);
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.of("foo", 123L), parameter.toLowParameter(closeableSet));
        assertEquals("foo=123(long)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfStringLongWrapper() {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (Long) null).toLowParameter(closeableSet));

        var parameter = TgBindParameter.of("foo", Long.valueOf(123));
        assertEquals(Parameters.of("foo", 123L), parameter.toLowParameter(closeableSet));
        assertEquals("foo=123(Long)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfStringFloat() {
        var parameter = TgBindParameter.of("foo", 123f);
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.of("foo", 123f), parameter.toLowParameter(closeableSet));
        assertEquals("foo=123.0(float)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfStringFloatWrapper() {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (Float) null).toLowParameter(closeableSet));

        var parameter = TgBindParameter.of("foo", Float.valueOf(123));
        assertEquals(Parameters.of("foo", 123f), parameter.toLowParameter(closeableSet));
        assertEquals("foo=123.0(Float)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfStringDouble() {
        var parameter = TgBindParameter.of("foo", 123d);
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.of("foo", 123d), parameter.toLowParameter(closeableSet));
        assertEquals("foo=123.0(double)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfStringDoubleWrapper() {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (Double) null).toLowParameter(closeableSet));

        var parameter = TgBindParameter.of("foo", Double.valueOf(123));
        assertEquals(Parameters.of("foo", 123d), parameter.toLowParameter(closeableSet));
        assertEquals("foo=123.0(Double)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfStringBigDecimal() {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (BigDecimal) null).toLowParameter(closeableSet));

        var parameter = TgBindParameter.of("foo", BigDecimal.valueOf(123));
        assertEquals(Parameters.of("foo", BigDecimal.valueOf(123)), parameter.toLowParameter(closeableSet));
        assertEquals("foo=123(BigDecimal)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfStringString() {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (String) null).toLowParameter(closeableSet));

        var parameter = TgBindParameter.of("foo", "abc");
        assertEquals(Parameters.of("foo", "abc"), parameter.toLowParameter(closeableSet));
        assertEquals("foo=abc(String)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfStringBytes() {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (byte[]) null).toLowParameter(closeableSet));

        var parameter = TgBindParameter.of("foo", new byte[] { 0, 1, 0xe, 0xf, 0x10, 0x7f, -1 });
        assertEquals(Parameters.of("foo", new byte[] { 0, 1, 0xe, 0xf, 0x10, 0x7f, -1 }), parameter.toLowParameter(closeableSet));
        assertEquals("foo=[00,01,0e,0f,10,7f,ff](byte[])", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfStringBooleanArray() {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (boolean[]) null).toLowParameter(closeableSet));

        var parameter = TgBindParameter.of("foo", new boolean[] { true, false, true });
        assertEquals(Parameters.of("foo", new boolean[] { true, false, true }), parameter.toLowParameter(closeableSet));
        assertEquals("foo=[true, false, true](boolean[])", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfStringLocalDate() {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (LocalDate) null).toLowParameter(closeableSet));

        var parameter = TgBindParameter.of("foo", LocalDate.of(2022, 6, 3));
        assertEquals(Parameters.of("foo", LocalDate.of(2022, 6, 3)), parameter.toLowParameter(closeableSet));
        assertEquals("foo=2022-06-03(LocalDate)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfStringLocalTime() {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (LocalTime) null).toLowParameter(closeableSet));

        var parameter = TgBindParameter.of("foo", LocalTime.of(23, 30, 59));
        assertEquals(Parameters.of("foo", LocalTime.of(23, 30, 59)), parameter.toLowParameter(closeableSet));
        assertEquals("foo=23:30:59(LocalTime)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfStringLocalDateTime() {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (LocalDateTime) null).toLowParameter(closeableSet));

        var parameter = TgBindParameter.of("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59));
        assertEquals(Parameters.of("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59)), parameter.toLowParameter(closeableSet));
        assertEquals("foo=2022-09-22T23:30:59(LocalDateTime)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfStringOffsetTime() {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (OffsetTime) null).toLowParameter(closeableSet));

        var offset = ZoneOffset.ofHours(9);
        var parameter = TgBindParameter.of("foo", OffsetTime.of(23, 30, 59, 0, offset));
        assertEquals(Parameters.of("foo", OffsetTime.of(23, 30, 59, 0, offset)), parameter.toLowParameter(closeableSet));
        assertEquals("foo=23:30:59+09:00(OffsetTime)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfStringOffsetDateTime() {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (OffsetDateTime) null).toLowParameter(closeableSet));

        var offset = ZoneOffset.ofHours(9);
        var parameter = TgBindParameter.of("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset));
        assertEquals(Parameters.of("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset)), parameter.toLowParameter(closeableSet));
        assertEquals("foo=2022-09-22T23:30:59+09:00(OffsetDateTime)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfStringZonedDateTime() {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (ZonedDateTime) null).toLowParameter(closeableSet));

        var zone = ZoneId.of("Asia/Tokyo");
        var dateTime = ZonedDateTime.of(2022, 6, 3, 23, 30, 59, 999, zone);
        var parameter = TgBindParameter.of("foo", dateTime);
        assertEquals(Parameters.of("foo", dateTime.toOffsetDateTime()), parameter.toLowParameter(closeableSet));
        assertEquals("foo=2022-06-03T23:30:59.000000999+09:00[Asia/Tokyo](ZonedDateTime)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfStringBlob() {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (TgBlob) null).toLowParameter(closeableSet));

        var path = Path.of("/path/to/file");
        var parameter = TgBindParameter.of("foo", TgBlob.of(path));
        assertEquals(Parameters.blobOf("foo", path), parameter.toLowParameter(closeableSet));
        assertEquals("foo=TgBlobPath(" + path + ")(TgBlob)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfBlobStringPath() {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.ofBlob("foo", (Path) null).toLowParameter(closeableSet));

        var path = Path.of("/path/to/file");
        var parameter = TgBindParameter.ofBlob("foo", path);
        assertEquals(Parameters.blobOf("foo", path), parameter.toLowParameter(closeableSet));
        assertEquals("foo=" + path + "(Path)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfBlobStringInputStream() throws Exception {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.ofBlob("foo", (InputStream) null).toLowParameter(closeableSet));
        assertEquals(0, closeableSet.size());

        var parameter = TgBindParameter.ofBlob("foo", new ByteArrayInputStream(new byte[] { 1, 2, 3 }));
        var actual = parameter.toLowParameter(closeableSet);

        try (var blob = TestBlobUtil.getBlob1(closeableSet)) {
            var path = blob.getPath();

            assertEquals(Parameters.blobOf("foo", path), actual);
        }
    }

    @Test
    void testOfBlobStringBytes() throws Exception {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.ofBlob("foo", (byte[]) null).toLowParameter(closeableSet));
        assertEquals(0, closeableSet.size());

        var parameter = TgBindParameter.ofBlob("foo", new byte[] { 1, 2, 3 });
        var actual = parameter.toLowParameter(closeableSet);

        try (var blob = TestBlobUtil.getBlob1(closeableSet)) {
            var path = blob.getPath();

            assertEquals(Parameters.blobOf("foo", path), actual);
        }
    }

    @Test
    void testOfStringClob() {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (TgClob) null).toLowParameter(closeableSet));

        var path = Path.of("/path/to/file");
        var parameter = TgBindParameter.of("foo", TgClob.of(path));
        assertEquals(Parameters.clobOf("foo", path), parameter.toLowParameter(closeableSet));
        assertEquals("foo=TgClobPath(" + path + ")(TgClob)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfClobStringPath() {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.ofClob("foo", (Path) null).toLowParameter(closeableSet));

        var path = Path.of("/path/to/file");
        var parameter = TgBindParameter.ofClob("foo", path);
        assertEquals(Parameters.clobOf("foo", path), parameter.toLowParameter(closeableSet));
        assertEquals("foo=" + path + "(Path)", parameter.toString());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfClobStringReader() throws Exception {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.ofClob("foo", (Reader) null).toLowParameter(closeableSet));
        assertEquals(0, closeableSet.size());

        var parameter = TgBindParameter.ofClob("foo", new StringReader("abc"));
        var actual = parameter.toLowParameter(closeableSet);

        try (var clob = TestClobUtil.getClob1(closeableSet)) {
            var path = clob.getPath();

            assertEquals(Parameters.clobOf("foo", path), actual);
        }
    }

    @Test
    void testOfClobStringString() throws Exception {
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.ofClob("foo", (String) null).toLowParameter(closeableSet));
        assertEquals(0, closeableSet.size());

        var parameter = TgBindParameter.ofClob("foo", "abc");
        var actual = parameter.toLowParameter(closeableSet);

        try (var clob = TestClobUtil.getClob1(closeableSet)) {
            var path = clob.getPath();

            assertEquals(Parameters.clobOf("foo", path), actual);
        }
    }
}
