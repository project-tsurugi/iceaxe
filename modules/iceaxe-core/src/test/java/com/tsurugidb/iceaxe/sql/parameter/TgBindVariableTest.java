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
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableBigDecimal;
import com.tsurugidb.iceaxe.sql.type.TgBlob;
import com.tsurugidb.iceaxe.sql.type.TgClob;
import com.tsurugidb.iceaxe.test.TestBlobUtil;
import com.tsurugidb.iceaxe.test.TestClobUtil;
import com.tsurugidb.iceaxe.util.IceaxeCloseableSet;
import com.tsurugidb.tsubakuro.sql.Parameters;

class TgBindVariableTest {

    @Test
    void testOfBoolean() {
        var variable = TgBindVariable.ofBoolean("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.BOOLEAN, variable.type());
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.of("foo", true), variable.bind(true).toLowParameter(closeableSet));
        assertEquals(Parameters.of("foo", true), variable.bind(Boolean.TRUE).toLowParameter(closeableSet));

        var copy = variable.clone("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfInt() {
        var variable = TgBindVariable.ofInt("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.INT, variable.type());
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.of("foo", 123), variable.bind(123).toLowParameter(closeableSet));
        assertEquals(Parameters.of("foo", 123), variable.bind(Integer.valueOf(123)).toLowParameter(closeableSet));

        var copy = variable.clone("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfLong() {
        var variable = TgBindVariable.ofLong("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.LONG, variable.type());
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.of("foo", 123L), variable.bind(123).toLowParameter(closeableSet));
        assertEquals(Parameters.of("foo", 123L), variable.bind(Long.valueOf(123)).toLowParameter(closeableSet));

        var copy = variable.clone("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfFloat() {
        var variable = TgBindVariable.ofFloat("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.FLOAT, variable.type());
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.of("foo", 123f), variable.bind(123).toLowParameter(closeableSet));
        assertEquals(Parameters.of("foo", 123f), variable.bind(Float.valueOf(123)).toLowParameter(closeableSet));

        var copy = variable.clone("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfDouble() {
        var variable = TgBindVariable.ofDouble("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.DOUBLE, variable.type());
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.of("foo", 123d), variable.bind(123).toLowParameter(closeableSet));
        assertEquals(Parameters.of("foo", 123d), variable.bind(Double.valueOf(123)).toLowParameter(closeableSet));

        var copy = variable.clone("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfDecimal() {
        var variable = TgBindVariable.ofDecimal("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.DECIMAL, variable.type());
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.of("foo", BigDecimal.valueOf(123)), variable.bind(BigDecimal.valueOf(123)).toLowParameter(closeableSet));
        assertEquals(":foo/*DECIMAL*/", variable.toString());

        var copy = variable.clone("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());

        assertEquals(0, closeableSet.size());
    }

    @ParameterizedTest
    @ValueSource(strings = { "1.01", "1.05", "-1.01", "-1.05", "" })
    void testOfDecimalScale(String value) {
        int scale = 1;
        var variable = TgBindVariable.ofDecimal("foo", scale);
        testOfDecimal(variable, value, scale, TgBindVariableBigDecimal.DEFAULT_ROUNDING_MODE);
    }

    @ParameterizedTest
    @ValueSource(strings = { "1.01", "1.05", "-1.01", "-1.05", "" })
    void testOfDecimalRoundingMode(String value) {
        int scale = 1;
        var mode = RoundingMode.HALF_UP;
        var variable = TgBindVariable.ofDecimal("foo", scale, mode);
        testOfDecimal(variable, value, scale, mode);
    }

    private void testOfDecimal(TgBindVariableBigDecimal variable, String s, int scale, RoundingMode mode) {
        var value = s.isEmpty() ? null : new BigDecimal(s);
        var expected = (value != null) ? value.setScale(scale, mode) : null;

        assertEquals("foo", variable.name());
        assertEquals(TgDataType.DECIMAL, variable.type());
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(IceaxeLowParameterUtil.create("foo", expected), variable.bind(value).toLowParameter(closeableSet));
        assertEquals(String.format(":foo/*DECIMAL<%s %d>*/", mode, scale), variable.toString());

        var copy = variable.clone("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());
        assertEquals(IceaxeLowParameterUtil.create("bar", expected), copy.bind(value).toLowParameter(closeableSet));

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testDecimalBind() {
        var variable = TgBindVariable.ofDecimal("foo");

        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.of("foo", BigDecimal.valueOf(123)), variable.bind(123).toLowParameter(closeableSet));
        assertEquals(Parameters.of("foo", BigDecimal.valueOf(123.4)), variable.bind(123.4).toLowParameter(closeableSet));

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfString() {
        var variable = TgBindVariable.ofString("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.STRING, variable.type());
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.of("foo", "abc"), variable.bind("abc").toLowParameter(closeableSet));

        var copy = variable.clone("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfBytes() {
        var variable = TgBindVariable.ofBytes("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.BYTES, variable.type());
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.of("foo", new byte[] { 1, 2, 3 }), variable.bind(new byte[] { 1, 2, 3 }).toLowParameter(closeableSet));

        var copy = variable.clone("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfBits() {
        var variable = TgBindVariable.ofBits("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.BITS, variable.type());
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.of("foo", new boolean[] { true, false, true }), variable.bind(new boolean[] { true, false, true }).toLowParameter(closeableSet));

        var copy = variable.clone("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfDate() {
        var variable = TgBindVariable.ofDate("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.DATE, variable.type());
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.of("foo", LocalDate.of(2022, 6, 2)), variable.bind(LocalDate.of(2022, 6, 2)).toLowParameter(closeableSet));

        var copy = variable.clone("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfTime() {
        var variable = TgBindVariable.ofTime("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.TIME, variable.type());
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.of("foo", LocalTime.of(23, 30, 59)), variable.bind(LocalTime.of(23, 30, 59)).toLowParameter(closeableSet));

        var copy = variable.clone("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfDateTime() {
        var variable = TgBindVariable.ofDateTime("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.DATE_TIME, variable.type());
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.of("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59)), variable.bind(LocalDateTime.of(2022, 9, 22, 23, 30, 59)).toLowParameter(closeableSet));

        var copy = variable.clone("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfOffsetTime() {
        var variable = TgBindVariable.ofOffsetTime("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.OFFSET_TIME, variable.type());
        var offset = ZoneOffset.ofHours(9);
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.of("foo", OffsetTime.of(23, 30, 59, 0, offset)), variable.bind(OffsetTime.of(23, 30, 59, 0, offset)).toLowParameter(closeableSet));

        var copy = variable.clone("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfOffsetDateTime() {
        var variable = TgBindVariable.ofOffsetDateTime("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.OFFSET_DATE_TIME, variable.type());
        var offset = ZoneOffset.ofHours(9);
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.of("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset)), variable.bind(OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset)).toLowParameter(closeableSet));

        var copy = variable.clone("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfZonedDateTime() {
        var variable = TgBindVariable.ofZonedDateTime("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.ZONED_DATE_TIME, variable.type());

        var zone = ZoneId.of("Asia/Tokyo");
        var dateTime = ZonedDateTime.of(2022, 6, 2, 23, 30, 59, 999, zone);
        var closeableSet = new IceaxeCloseableSet();
        assertEquals(Parameters.of("foo", dateTime.toOffsetDateTime()), variable.bind(dateTime).toLowParameter(closeableSet));

        var copy = variable.clone("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());

        assertEquals(0, closeableSet.size());
    }

    @Test
    void testOfBlob() throws Exception {
        var variable = TgBindVariable.ofBlob("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.BLOB, variable.type());
        {
            var closeableSet = new IceaxeCloseableSet();
            assertEquals(Parameters.blobOf("foo", Path.of("/path/to/file")), variable.bind(TgBlob.of(Path.of("/path/to/file"))).toLowParameter(closeableSet));
            assertEquals(0, closeableSet.size());
        }
        {
            var closeableSet = new IceaxeCloseableSet();
            assertEquals(Parameters.blobOf("foo", Path.of("/path/to/file")), variable.bind(Path.of("/path/to/file")).toLowParameter(closeableSet));
            assertEquals(0, closeableSet.size());
        }
        {
            var closeableSet = new IceaxeCloseableSet();
            var actual = variable.bind(new ByteArrayInputStream(new byte[] { 1, 2, 3 })).toLowParameter(closeableSet);

            try (var blob = TestBlobUtil.getBlob1(closeableSet)) {
                var path = blob.getPath();

                assertEquals(Parameters.blobOf("foo", path), actual);
            }
        }
        {
            var closeableSet = new IceaxeCloseableSet();
            var actual = variable.bind(new byte[] { 1, 2, 3 }).toLowParameter(closeableSet);

            try (var blob = TestBlobUtil.getBlob1(closeableSet)) {
                var path = blob.getPath();

                assertEquals(Parameters.blobOf("foo", path), actual);
            }
        }

        var copy = variable.clone("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());
    }

    @Test
    void testOfClob() throws Exception {
        var variable = TgBindVariable.ofClob("foo");
        assertEquals("foo", variable.name());
        assertEquals(TgDataType.CLOB, variable.type());
        {
            var closeableSet = new IceaxeCloseableSet();
            assertEquals(Parameters.clobOf("foo", Path.of("/path/to/file")), variable.bind(TgClob.of(Path.of("/path/to/file"))).toLowParameter(closeableSet));
            assertEquals(0, closeableSet.size());
        }
        {
            var closeableSet = new IceaxeCloseableSet();
            assertEquals(Parameters.clobOf("foo", Path.of("/path/to/file")), variable.bind(Path.of("/path/to/file")).toLowParameter(closeableSet));
            assertEquals(0, closeableSet.size());
        }
        {
            var closeableSet = new IceaxeCloseableSet();
            var actual = variable.bind(new StringReader("abc")).toLowParameter(closeableSet);

            try (var clob = TestClobUtil.getClob1(closeableSet)) {
                var path = clob.getPath();

                assertEquals(Parameters.clobOf("foo", path), actual);
            }
        }
        {
            var closeableSet = new IceaxeCloseableSet();
            var actual = variable.bind("abc").toLowParameter(closeableSet);

            try (var clob = TestClobUtil.getClob1(closeableSet)) {
                var path = clob.getPath();

                assertEquals(Parameters.clobOf("foo", path), actual);
            }
        }

        var copy = variable.clone("bar");
        assertEquals(variable.getClass(), copy.getClass());
        assertEquals("bar", copy.name());
        assertEquals(variable.type(), copy.type());
    }

    @Test
    void testSqlName() {
        var variable = TgBindVariable.ofInt("foo");
        assertEquals(":foo", variable.sqlName());
    }

    @Test
    void testToString() {
        var variable = TgBindVariable.ofInt("foo");
        assertEquals(":foo/*INT*/", variable.toString());
    }
}
