/*
 * Copyright 2023-2024 Project Tsurugi.
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

class TgBindParameterTest {

    @Test
    void testOfStringBoolean() {
        var parameter = TgBindParameter.of("foo", true);
        assertEquals(Parameters.of("foo", true), parameter.toLowParameter());
        assertEquals("foo=true(boolean)", parameter.toString());
    }

    @Test
    void testOfStringBooleanWrapper() {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (Boolean) null).toLowParameter());

        var parameter = TgBindParameter.of("foo", Boolean.TRUE);
        assertEquals(Parameters.of("foo", true), parameter.toLowParameter());
        assertEquals("foo=true(Boolean)", parameter.toString());
    }

    @Test
    void testOfStringInt() {
        var parameter = TgBindParameter.of("foo", 123);
        assertEquals(Parameters.of("foo", 123), parameter.toLowParameter());
        assertEquals("foo=123(int)", parameter.toString());
    }

    @Test
    void testOfStringInteger() {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (Integer) null).toLowParameter());

        var parameter = TgBindParameter.of("foo", Integer.valueOf(123));
        assertEquals(Parameters.of("foo", 123), parameter.toLowParameter());
        assertEquals("foo=123(Integer)", parameter.toString());
    }

    @Test
    void testOfStringLong() {
        var parameter = TgBindParameter.of("foo", 123L);
        assertEquals(Parameters.of("foo", 123L), parameter.toLowParameter());
        assertEquals("foo=123(long)", parameter.toString());
    }

    @Test
    void testOfStringLongWrapper() {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (Long) null).toLowParameter());

        var parameter = TgBindParameter.of("foo", Long.valueOf(123));
        assertEquals(Parameters.of("foo", 123L), parameter.toLowParameter());
        assertEquals("foo=123(Long)", parameter.toString());
    }

    @Test
    void testOfStringFloat() {
        var parameter = TgBindParameter.of("foo", 123f);
        assertEquals(Parameters.of("foo", 123f), parameter.toLowParameter());
        assertEquals("foo=123.0(float)", parameter.toString());
    }

    @Test
    void testOfStringFloatWrapper() {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (Float) null).toLowParameter());

        var parameter = TgBindParameter.of("foo", Float.valueOf(123));
        assertEquals(Parameters.of("foo", 123f), parameter.toLowParameter());
        assertEquals("foo=123.0(Float)", parameter.toString());
    }

    @Test
    void testOfStringDouble() {
        var parameter = TgBindParameter.of("foo", 123d);
        assertEquals(Parameters.of("foo", 123d), parameter.toLowParameter());
        assertEquals("foo=123.0(double)", parameter.toString());
    }

    @Test
    void testOfStringDoubleWrapper() {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (Double) null).toLowParameter());

        var parameter = TgBindParameter.of("foo", Double.valueOf(123));
        assertEquals(Parameters.of("foo", 123d), parameter.toLowParameter());
        assertEquals("foo=123.0(Double)", parameter.toString());
    }

    @Test
    void testOfStringBigDecimal() {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (BigDecimal) null).toLowParameter());

        var parameter = TgBindParameter.of("foo", BigDecimal.valueOf(123));
        assertEquals(Parameters.of("foo", BigDecimal.valueOf(123)), parameter.toLowParameter());
        assertEquals("foo=123(BigDecimal)", parameter.toString());
    }

    @Test
    void testOfStringString() {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (String) null).toLowParameter());

        var parameter = TgBindParameter.of("foo", "abc");
        assertEquals(Parameters.of("foo", "abc"), parameter.toLowParameter());
        assertEquals("foo=abc(String)", parameter.toString());
    }

    @Test
    void testOfStringByteArray() {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (byte[]) null).toLowParameter());

        var parameter = TgBindParameter.of("foo", new byte[] { 0, 1, 0xe, 0xf, 0x10, 0x7f, -1 });
        assertEquals(Parameters.of("foo", new byte[] { 0, 1, 0xe, 0xf, 0x10, 0x7f, -1 }), parameter.toLowParameter());
        assertEquals("foo=[00,01,0e,0f,10,7f,ff](byte[])", parameter.toString());
    }

    @Test
    void testOfStringBooleanArray() {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (boolean[]) null).toLowParameter());

        var parameter = TgBindParameter.of("foo", new boolean[] { true, false, true });
        assertEquals(Parameters.of("foo", new boolean[] { true, false, true }), parameter.toLowParameter());
        assertEquals("foo=[true, false, true](boolean[])", parameter.toString());
    }

    @Test
    void testOfStringLocalDate() {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (LocalDate) null).toLowParameter());

        var parameter = TgBindParameter.of("foo", LocalDate.of(2022, 6, 3));
        assertEquals(Parameters.of("foo", LocalDate.of(2022, 6, 3)), parameter.toLowParameter());
        assertEquals("foo=2022-06-03(LocalDate)", parameter.toString());
    }

    @Test
    void testOfStringLocalTime() {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (LocalTime) null).toLowParameter());

        var parameter = TgBindParameter.of("foo", LocalTime.of(23, 30, 59));
        assertEquals(Parameters.of("foo", LocalTime.of(23, 30, 59)), parameter.toLowParameter());
        assertEquals("foo=23:30:59(LocalTime)", parameter.toString());
    }

    @Test
    void testOfStringLocalDateTime() {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (LocalDateTime) null).toLowParameter());

        var parameter = TgBindParameter.of("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59));
        assertEquals(Parameters.of("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59)), parameter.toLowParameter());
        assertEquals("foo=2022-09-22T23:30:59(LocalDateTime)", parameter.toString());
    }

    @Test
    void testOfStringOffsetTime() {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (OffsetTime) null).toLowParameter());

        var offset = ZoneOffset.ofHours(9);
        var parameter = TgBindParameter.of("foo", OffsetTime.of(23, 30, 59, 0, offset));
        assertEquals(Parameters.of("foo", OffsetTime.of(23, 30, 59, 0, offset)), parameter.toLowParameter());
        assertEquals("foo=23:30:59+09:00(OffsetTime)", parameter.toString());
    }

    @Test
    void testOfStringOffsetDateTime() {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (OffsetDateTime) null).toLowParameter());

        var offset = ZoneOffset.ofHours(9);
        var parameter = TgBindParameter.of("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset));
        assertEquals(Parameters.of("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset)), parameter.toLowParameter());
        assertEquals("foo=2022-09-22T23:30:59+09:00(OffsetDateTime)", parameter.toString());
    }

    @Test
    void testOfStringZonedDateTime() {
        assertEquals(Parameters.ofNull("foo"), TgBindParameter.of("foo", (ZonedDateTime) null).toLowParameter());

        var zone = ZoneId.of("Asia/Tokyo");
        var dateTime = ZonedDateTime.of(2022, 6, 3, 23, 30, 59, 999, zone);
        var parameter = TgBindParameter.of("foo", dateTime);
        assertEquals(Parameters.of("foo", dateTime.toOffsetDateTime()), parameter.toLowParameter());
        assertEquals("foo=2022-06-03T23:30:59.000000999+09:00[Asia/Tokyo](ZonedDateTime)", parameter.toString());
    }
}
