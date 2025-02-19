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
    void testCreateStringLocalDateTime() {
        assertEquals(Parameters.ofNull("foo"), IceaxeLowParameterUtil.create("foo", (LocalDateTime) null));

        var parameter = IceaxeLowParameterUtil.create("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59));
        assertEquals(Parameters.of("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59)), parameter);
    }

    @Test
    void testCreateStringOffsetTime() {
        assertEquals(Parameters.ofNull("foo"), IceaxeLowParameterUtil.create("foo", (OffsetTime) null));

        var offset = ZoneOffset.ofHours(9);
        var parameter = IceaxeLowParameterUtil.create("foo", OffsetTime.of(23, 30, 59, 999, offset));
        assertEquals(Parameters.of("foo", OffsetTime.of(23, 30, 59, 999, offset)), parameter);
    }

    @Test
    void testCreateStringOffsetDateTime() {
        assertEquals(Parameters.ofNull("foo"), IceaxeLowParameterUtil.create("foo", (OffsetDateTime) null));

        var offset = ZoneOffset.ofHours(9);
        var parameter = IceaxeLowParameterUtil.create("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 999, offset));
        assertEquals(Parameters.of("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 999, offset)), parameter);
    }

    @Test
    void testCreateStringZonedDateTime() {
        assertEquals(Parameters.ofNull("foo"), IceaxeLowParameterUtil.create("foo", (ZonedDateTime) null));

        var zone = ZoneId.of("Asia/Tokyo");
        var dateTime = ZonedDateTime.of(2022, 6, 3, 23, 30, 59, 999, zone);
        var parameter = IceaxeLowParameterUtil.create("foo", dateTime);
        assertEquals(Parameters.of("foo", dateTime.toOffsetDateTime()), parameter);
    }

    @Test
    void testCreateStringBlob() {
        assertEquals(Parameters.ofNull("foo"), IceaxeLowParameterUtil.create("foo", (TgBlob) null));

        var parameter = IceaxeLowParameterUtil.create("foo", TgBlob.of(Path.of("/path/to/flie")));
        assertEquals(Parameters.blobOf("foo", Path.of("/path/to/flie")), parameter);
    }

    @Test
    void testCreateBlobStringPath() {
        assertEquals(Parameters.ofNull("foo"), IceaxeLowParameterUtil.createBlob("foo", (Path) null));

        var parameter = IceaxeLowParameterUtil.createBlob("foo", Path.of("/path/to/flie"));
        assertEquals(Parameters.blobOf("foo", Path.of("/path/to/flie")), parameter);
    }

    // TODO CLOB
}
