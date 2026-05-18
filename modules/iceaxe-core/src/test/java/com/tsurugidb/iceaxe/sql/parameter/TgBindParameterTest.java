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
import com.tsurugidb.iceaxe.sql.type.TgRemoteBlob;
import com.tsurugidb.iceaxe.sql.type.TgRemoteBlobInfo;
import com.tsurugidb.iceaxe.sql.type.TgRemoteClob;
import com.tsurugidb.iceaxe.sql.type.TgRemoteClobInfo;
import com.tsurugidb.iceaxe.test.TestLowParameterGenerateContextWrapper;
import com.tsurugidb.tsubakuro.sql.Parameters;

class TgBindParameterTest {

    @Test
    void testOfStringBoolean() throws Exception {
        var parameter = TgBindParameter.of("foo", true);
        assertEquals(Parameters.of("foo", true), parameter.toLowParameter(null));
        assertEquals("foo=true(boolean)", parameter.toString());
    }

    @Test
    void testOfStringBooleanWrapper() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (Boolean) null).toLowParameter(null));

        var parameter = TgBindParameter.of("foo", Boolean.TRUE);
        assertEquals(Parameters.of("foo", true), parameter.toLowParameter(null));
        assertEquals("foo=true(Boolean)", parameter.toString());
    }

    @Test
    void testOfStringInt() throws Exception {
        var parameter = TgBindParameter.of("foo", 123);
        assertEquals(Parameters.of("foo", 123), parameter.toLowParameter(null));
        assertEquals("foo=123(int)", parameter.toString());
    }

    @Test
    void testOfStringInteger() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (Integer) null).toLowParameter(null));

        var parameter = TgBindParameter.of("foo", Integer.valueOf(123));
        assertEquals(Parameters.of("foo", 123), parameter.toLowParameter(null));
        assertEquals("foo=123(Integer)", parameter.toString());
    }

    @Test
    void testOfStringLong() throws Exception {
        var parameter = TgBindParameter.of("foo", 123L);
        assertEquals(Parameters.of("foo", 123L), parameter.toLowParameter(null));
        assertEquals("foo=123(long)", parameter.toString());
    }

    @Test
    void testOfStringLongWrapper() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (Long) null).toLowParameter(null));

        var parameter = TgBindParameter.of("foo", Long.valueOf(123));
        assertEquals(Parameters.of("foo", 123L), parameter.toLowParameter(null));
        assertEquals("foo=123(Long)", parameter.toString());
    }

    @Test
    void testOfStringFloat() throws Exception {
        var parameter = TgBindParameter.of("foo", 123f);
        assertEquals(Parameters.of("foo", 123f), parameter.toLowParameter(null));
        assertEquals("foo=123.0(float)", parameter.toString());
    }

    @Test
    void testOfStringFloatWrapper() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (Float) null).toLowParameter(null));

        var parameter = TgBindParameter.of("foo", Float.valueOf(123));
        assertEquals(Parameters.of("foo", 123f), parameter.toLowParameter(null));
        assertEquals("foo=123.0(Float)", parameter.toString());
    }

    @Test
    void testOfStringDouble() throws Exception {
        var parameter = TgBindParameter.of("foo", 123d);
        assertEquals(Parameters.of("foo", 123d), parameter.toLowParameter(null));
        assertEquals("foo=123.0(double)", parameter.toString());
    }

    @Test
    void testOfStringDoubleWrapper() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (Double) null).toLowParameter(null));

        var parameter = TgBindParameter.of("foo", Double.valueOf(123));
        assertEquals(Parameters.of("foo", 123d), parameter.toLowParameter(null));
        assertEquals("foo=123.0(Double)", parameter.toString());
    }

    @Test
    void testOfStringBigDecimal() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (BigDecimal) null).toLowParameter(null));

        var parameter = TgBindParameter.of("foo", BigDecimal.valueOf(123));
        assertEquals(Parameters.of("foo", BigDecimal.valueOf(123)), parameter.toLowParameter(null));
        assertEquals("foo=123(BigDecimal)", parameter.toString());
    }

    @Test
    void testOfStringString() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (String) null).toLowParameter(null));

        var parameter = TgBindParameter.of("foo", "abc");
        assertEquals(Parameters.of("foo", "abc"), parameter.toLowParameter(null));
        assertEquals("foo=abc(String)", parameter.toString());
    }

    @Test
    void testOfStringBytes() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (byte[]) null).toLowParameter(null));

        var parameter = TgBindParameter.of("foo", new byte[] { 0, 1, 0xe, 0xf, 0x10, 0x7f, -1 });
        assertEquals(Parameters.of("foo", new byte[] { 0, 1, 0xe, 0xf, 0x10, 0x7f, -1 }), parameter.toLowParameter(null));
        assertEquals("foo=[00,01,0e,0f,10,7f,ff](byte[])", parameter.toString());
    }

    @Test
    void testOfStringBooleanArray() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (boolean[]) null).toLowParameter(null));

        var parameter = TgBindParameter.of("foo", new boolean[] { true, false, true });
        assertEquals(Parameters.of("foo", new boolean[] { true, false, true }), parameter.toLowParameter(null));
        assertEquals("foo=[true, false, true](boolean[])", parameter.toString());
    }

    @Test
    void testOfStringLocalDate() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (LocalDate) null).toLowParameter(null));

        var parameter = TgBindParameter.of("foo", LocalDate.of(2022, 6, 3));
        assertEquals(Parameters.of("foo", LocalDate.of(2022, 6, 3)), parameter.toLowParameter(null));
        assertEquals("foo=2022-06-03(LocalDate)", parameter.toString());
    }

    @Test
    void testOfStringLocalTime() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (LocalTime) null).toLowParameter(null));

        var parameter = TgBindParameter.of("foo", LocalTime.of(23, 30, 59));
        assertEquals(Parameters.of("foo", LocalTime.of(23, 30, 59)), parameter.toLowParameter(null));
        assertEquals("foo=23:30:59(LocalTime)", parameter.toString());
    }

    @Test
    void testOfStringLocalDateTime() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (LocalDateTime) null).toLowParameter(null));

        var parameter = TgBindParameter.of("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59));
        assertEquals(Parameters.of("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59)), parameter.toLowParameter(null));
        assertEquals("foo=2022-09-22T23:30:59(LocalDateTime)", parameter.toString());
    }

    @Test
    void testOfStringOffsetTime() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (OffsetTime) null).toLowParameter(null));

        var offset = ZoneOffset.ofHours(9);
        var parameter = TgBindParameter.of("foo", OffsetTime.of(23, 30, 59, 0, offset));
        assertEquals(Parameters.of("foo", OffsetTime.of(23, 30, 59, 0, offset)), parameter.toLowParameter(null));
        assertEquals("foo=23:30:59+09:00(OffsetTime)", parameter.toString());
    }

    @Test
    void testOfStringOffsetDateTime() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (OffsetDateTime) null).toLowParameter(null));

        var offset = ZoneOffset.ofHours(9);
        var parameter = TgBindParameter.of("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset));
        assertEquals(Parameters.of("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset)), parameter.toLowParameter(null));
        assertEquals("foo=2022-09-22T23:30:59+09:00(OffsetDateTime)", parameter.toString());
    }

    @Test
    void testOfStringZonedDateTime() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (ZonedDateTime) null).toLowParameter(null));

        var zone = ZoneId.of("Asia/Tokyo");
        var dateTime = ZonedDateTime.of(2022, 6, 3, 23, 30, 59, 999, zone);
        var parameter = TgBindParameter.of("foo", dateTime);
        assertEquals(Parameters.of("foo", dateTime.toOffsetDateTime()), parameter.toLowParameter(null));
        assertEquals("foo=2022-06-03T23:30:59.000000999+09:00[Asia/Tokyo](ZonedDateTime)", parameter.toString());
    }

    @Test
    void testOfStringBlob() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (TgBlob) null).toLowParameter(null));

        var path = Path.of("/path/to/file");
        var parameter = TgBindParameter.of("foo", TgBlob.of(path));

        var contextWrapper = new TestLowParameterGenerateContextWrapper();
        var context = contextWrapper.context();
        var actual = parameter.toLowParameter(context);

        var lobInfo = contextWrapper.lowLargeObjectInfo();
        assertEquals(Parameters.blobOf("foo", lobInfo), actual);
        assertEquals("foo=TgBlobPath(" + path + ")(TgBlob)", parameter.toString());
    }

    @Test
    void testOfStringRemoteBlob() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (TgRemoteBlob) null).toLowParameter(null));

        var contextWrapper = new TestLowParameterGenerateContextWrapper();
        var context = contextWrapper.context();
        var path = Path.of("/path/to/file");
        var lobInfo = contextWrapper.createLowLargeObjectInfo(path);

        var parameter = TgBindParameter.of("foo", new TgRemoteBlobInfo(lobInfo));

        assertEquals(Parameters.blobOf("foo", lobInfo), parameter.toLowParameter(context));
        assertEquals("foo=TgRemoteBlobInfo(LargeObjectInfo(" + path + "))(TgRemoteBlob)", parameter.toString());
    }

    @Test
    void testOfBlobStringPath() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.ofBlob("foo", (Path) null).toLowParameter(null));

        var path = Path.of("/path/to/file");
        var parameter = TgBindParameter.ofBlob("foo", path);

        var contextWrapper = new TestLowParameterGenerateContextWrapper();
        var context = contextWrapper.context();
        var actual = parameter.toLowParameter(context);

        var lobInfo = contextWrapper.lowLargeObjectInfo();
        assertEquals(Parameters.blobOf("foo", lobInfo), actual);
        assertEquals("foo=" + path + "(Path)", parameter.toString());
    }

    @Test
    void testOfBlobStringInputStream() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.ofBlob("foo", (InputStream) null).toLowParameter(null));

        var is = new ByteArrayInputStream(new byte[] { 1, 2, 3 });
        var parameter = TgBindParameter.ofBlob("foo", is);

        var contextWrapper = new TestLowParameterGenerateContextWrapper();
        var context = contextWrapper.context();
        var actual = parameter.toLowParameter(context);

        var lobInfo = contextWrapper.lowLargeObjectInfo();
        assertEquals(Parameters.blobOf("foo", lobInfo), actual);
        assertEquals("foo=" + is + "(InputStream)", parameter.toString());
    }

    @Test
    void testOfBlobStringBytes() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.ofBlob("foo", (byte[]) null).toLowParameter(null));

        var value = new byte[] { 1, 2, 3 };
        var parameter = TgBindParameter.ofBlob("foo", value);

        var contextWrapper = new TestLowParameterGenerateContextWrapper();
        var context = contextWrapper.context();
        var actual = parameter.toLowParameter(context);

        var lobInfo = contextWrapper.lowLargeObjectInfo();
        assertEquals(Parameters.blobOf("foo", lobInfo), actual);
        assertEquals("foo=" + value + "(byte[])", parameter.toString());
    }

    @Test
    void testOfStringClob() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (TgClob) null).toLowParameter(null));

        var path = Path.of("/path/to/file");
        var parameter = TgBindParameter.of("foo", TgClob.of(path));

        var contextWrapper = new TestLowParameterGenerateContextWrapper();
        var context = contextWrapper.context();
        var actual = parameter.toLowParameter(context);

        var lobInfo = contextWrapper.lowLargeObjectInfo();
        assertEquals(Parameters.clobOf("foo", lobInfo), actual);
        assertEquals("foo=TgClobPath(" + path + ")(TgClob)", parameter.toString());
    }

    @Test
    void testOfStringRemoteClob() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (TgRemoteClob) null).toLowParameter(null));

        var contextWrapper = new TestLowParameterGenerateContextWrapper();
        var context = contextWrapper.context();
        var path = Path.of("/path/to/file");
        var lobInfo = contextWrapper.createLowLargeObjectInfo(path);

        var parameter = TgBindParameter.of("foo", new TgRemoteClobInfo(lobInfo));

        assertEquals(Parameters.clobOf("foo", lobInfo), parameter.toLowParameter(context));
        assertEquals("foo=TgRemoteClobInfo(LargeObjectInfo(" + path + "))(TgRemoteClob)", parameter.toString());
    }

    @Test
    void testOfClobStringPath() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.ofClob("foo", (Path) null).toLowParameter(null));

        var path = Path.of("/path/to/file");
        var parameter = TgBindParameter.ofClob("foo", path);

        var contextWrapper = new TestLowParameterGenerateContextWrapper();
        var context = contextWrapper.context();
        var actual = parameter.toLowParameter(context);

        var lobInfo = contextWrapper.lowLargeObjectInfo();
        assertEquals(Parameters.clobOf("foo", lobInfo), actual);
        assertEquals("foo=" + path + "(Path)", parameter.toString());
    }

    @Test
    void testOfClobStringReader() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.ofClob("foo", (Reader) null).toLowParameter(null));

        var reader = new StringReader("abc");
        var parameter = TgBindParameter.ofClob("foo", reader);

        var contextWrapper = new TestLowParameterGenerateContextWrapper();
        var context = contextWrapper.context();
        var actual = parameter.toLowParameter(context);

        var lobInfo = contextWrapper.lowLargeObjectInfo();
        assertEquals(Parameters.clobOf("foo", lobInfo), actual);
        assertEquals("foo=" + reader + "(Reader)", parameter.toString());
    }

    @Test
    void testOfClobStringString() throws Exception {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.ofClob("foo", (String) null).toLowParameter(null));

        var value = "abc";
        var parameter = TgBindParameter.ofClob("foo", value);

        var contextWrapper = new TestLowParameterGenerateContextWrapper();
        var context = contextWrapper.context();
        var actual = parameter.toLowParameter(context);

        var lobInfo = contextWrapper.lowLargeObjectInfo();
        assertEquals(Parameters.clobOf("foo", lobInfo), actual);
        assertEquals("foo=" + value + "(String)", parameter.toString());
    }
}
