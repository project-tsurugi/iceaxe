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
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableBigDecimal;
import com.tsurugidb.iceaxe.sql.type.TgBlob;
import com.tsurugidb.iceaxe.sql.type.TgClob;
import com.tsurugidb.iceaxe.test.TestLowParameterGenerateContextWrapper;
import com.tsurugidb.iceaxe.util.IceaxeFileUtil;

class TgBindParametersTest {

    @Test
    void testOf() throws Exception {
        var parameter = TgBindParameters.of();

        assertParameterList(List.of(), parameter);
    }

    @Test
    void testOfArray() throws Exception {
        var foo = TgBindVariable.ofInt("foo");
        var parameter = TgBindParameters.of(foo.bind(123));

        assertParameterList(TgBindParameter.of("foo", 123), parameter);
    }

    @Test
    void testOfCollection() throws Exception {
        var foo = TgBindVariable.ofInt("foo");
        var list = List.of(foo.bind(123));
        var parameter = TgBindParameters.of(list);

        assertParameterList(TgBindParameter.of("foo", 123), parameter);
    }

    @Test
    void testBoolStringBoolean() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.addBoolean("foo", true);

        assertParameterList(TgBindParameter.of("foo", true), parameter);
    }

    @Test
    void testBoolStringBooleanWrapper() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.addBoolean("foo", Boolean.TRUE);
        parameter.addBoolean("bar", null);

        assertParameterList(TgBindParameter.of("foo", Boolean.TRUE), TgBindParameter.of("bar", (Boolean) null), parameter);
    }

    @Test
    void testIntStringInt() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.addInt("foo", 123);

        assertParameterList(TgBindParameter.of("foo", 123), parameter);
    }

    @Test
    void testIntStringInteger() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.addInt("foo", Integer.valueOf(123));
        parameter.addInt("bar", null);

        assertParameterList(TgBindParameter.of("foo", 123), TgBindParameter.of("bar", (Integer) null), parameter);
    }

    @Test
    void testLongStringLong() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.addLong("foo", 123);

        assertParameterList(TgBindParameter.of("foo", 123L), parameter);
    }

    @Test
    void testLongStringLongWrapper() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.addLong("foo", Long.valueOf(123));
        parameter.addLong("bar", null);

        assertParameterList(TgBindParameter.of("foo", 123L), TgBindParameter.of("bar", (Long) null), parameter);
    }

    @Test
    void testFloatStringFloat() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.addFloat("foo", 123);

        assertParameterList(TgBindParameter.of("foo", 123f), parameter);
    }

    @Test
    void testFloatStringFloatWrapper() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.addFloat("foo", Float.valueOf(123));
        parameter.addFloat("bar", null);

        assertParameterList(TgBindParameter.of("foo", 123f), TgBindParameter.of("bar", (Float) null), parameter);
    }

    @Test
    void testDoubleStringDouble() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.addDouble("foo", 123);

        assertParameterList(TgBindParameter.of("foo", 123d), parameter);
    }

    @Test
    void testDoubleStringDoubleWrapper() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.addDouble("foo", Double.valueOf(123));
        parameter.addDouble("bar", null);

        assertParameterList(TgBindParameter.of("foo", 123d), TgBindParameter.of("bar", (Double) null), parameter);
    }

    @Test
    void testDecimal() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.addDecimal("foo", BigDecimal.valueOf(123));
        parameter.addDecimal("bar", null);

        assertParameterList(TgBindParameter.of("foo", BigDecimal.valueOf(123)), TgBindParameter.of("bar", (BigDecimal) null), parameter);
    }

    @Test
    void testDecimalScale() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.addDecimal("p", new BigDecimal("1.01"), 1);
        parameter.addDecimal("m", new BigDecimal("-1.01"), 1);
        parameter.addDecimal("bar", null, 1);

        var mode = TgBindVariableBigDecimal.DEFAULT_ROUNDING_MODE;
        assertParameterList(TgBindParameter.of("p", new BigDecimal("1.01").setScale(1, mode)), TgBindParameter.of("m", new BigDecimal("-1.01").setScale(1, mode)),
                TgBindParameter.of("bar", (BigDecimal) null), parameter);
    }

    @ParameterizedTest
    @ValueSource(strings = { "FLOOR", "DOWN", "HALF_UP" })
    void testDecimalRoundingMode(RoundingMode mode) throws Exception {
        var parameter = TgBindParameters.of();
        parameter.addDecimal("p", new BigDecimal("1.05"), 1, mode);
        parameter.addDecimal("m", new BigDecimal("-1.05"), 1, mode);
        parameter.addDecimal("bar", null, 1, mode);

        assertParameterList(TgBindParameter.of("p", new BigDecimal("1.05").setScale(1, mode)), TgBindParameter.of("m", new BigDecimal("-1.05").setScale(1, mode)),
                TgBindParameter.of("bar", (BigDecimal) null), parameter);
    }

    @Test
    void testString() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.addString("foo", "abc");
        parameter.addString("bar", null);

        assertParameterList(TgBindParameter.of("foo", "abc"), TgBindParameter.of("bar", (String) null), parameter);
    }

    @Test
    void testBytes() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.addBytes("foo", new byte[] { 1, 2, 3 });
        parameter.addBytes("bar", null);

        assertParameterList(TgBindParameter.of("foo", new byte[] { 1, 2, 3 }), TgBindParameter.of("bar", (byte[]) null), parameter);
    }

    @Test
    void testBits() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.addBits("foo", new boolean[] { true, false, true });
        parameter.addBits("bar", null);

        assertParameterList(TgBindParameter.of("foo", new boolean[] { true, false, true }), TgBindParameter.of("bar", (boolean[]) null), parameter);
    }

    @Test
    void testDate() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.addDate("foo", LocalDate.of(2022, 6, 30));
        parameter.addDate("bar", null);

        assertParameterList(TgBindParameter.of("foo", LocalDate.of(2022, 6, 30)), TgBindParameter.of("bar", (LocalDate) null), parameter);
    }

    @Test
    void testTime() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.addTime("foo", LocalTime.of(23, 30, 59));
        parameter.addTime("bar", null);

        assertParameterList(TgBindParameter.of("foo", LocalTime.of(23, 30, 59)), TgBindParameter.of("bar", (LocalTime) null), parameter);
    }

    @Test
    void testDateTime() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.addDateTime("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59));
        parameter.addDateTime("bar", null);

        assertParameterList(TgBindParameter.of("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59)), TgBindParameter.of("bar", (LocalDateTime) null), parameter);
    }

    @Test
    void testOffsetTime() throws Exception {
        var offset = ZoneOffset.ofHours(9);
        var parameter = TgBindParameters.of();
        parameter.addOffsetTime("foo", OffsetTime.of(23, 30, 59, 0, offset));
        parameter.addOffsetTime("bar", null);

        assertParameterList(TgBindParameter.of("foo", OffsetTime.of(23, 30, 59, 0, offset)), TgBindParameter.of("bar", (OffsetTime) null), parameter);
    }

    @Test
    void testOffsetDateTime() throws Exception {
        var offset = ZoneOffset.ofHours(9);
        var parameter = TgBindParameters.of();
        parameter.addOffsetDateTime("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset));
        parameter.addOffsetDateTime("bar", null);

        assertParameterList(TgBindParameter.of("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset)), TgBindParameter.of("bar", (OffsetTime) null), parameter);
    }

    @Test
    void testZonedDateTime() throws Exception {
        var zdt = ZonedDateTime.of(2022, 6, 30, 23, 30, 59, 999, ZoneId.of("Asia/Tokyo"));
        var parameter = TgBindParameters.of();
        parameter.addZonedDateTime("foo", zdt);
        parameter.addZonedDateTime("bar", null);

        assertParameterList(TgBindParameter.of("foo", zdt), TgBindParameter.of("bar", (ZonedDateTime) null), parameter);
    }

    @Test
    void testBlob() throws Exception {
        {
            var path = createTempFile();
            Files.write(path, new byte[] { 1, 2, 3 });
            var blob = TgBlob.of(path);
            var parameter = TgBindParameters.of();
            parameter.addBlob("foo", blob);
            parameter.addBlob("bar", (TgBlob) null);

            assertParameterListBlob(TgBindParameter.of("foo", blob), TgBindParameter.of("bar", (TgBlob) null), parameter, false);
        }
        {
            var path = createTempFile();
            Files.write(path, new byte[] { 1, 2, 3 });
            var parameter = TgBindParameters.of();
            parameter.addBlob("foo", path);
            parameter.addBlob("bar", (Path) null);

            assertParameterListBlob(TgBindParameter.ofBlob("foo", path), TgBindParameter.ofBlob("bar", (Path) null), parameter, false);
        }
        {
            var parameter = TgBindParameters.of();
            parameter.addBlob("foo", new ByteArrayInputStream(new byte[] { 1, 2, 3 }));
            parameter.addBlob("bar", (InputStream) null);

            assertParameterListBlob(TgBindParameter.ofBlob("foo", new ByteArrayInputStream(new byte[] { 1, 2, 3 })), TgBindParameter.ofBlob("bar", (InputStream) null), parameter, true);
        }
        {
            var value = new byte[] { 1, 2, 3 };
            var parameter = TgBindParameters.of();
            parameter.addBlob("foo", value);
            parameter.addBlob("bar", (byte[]) null);

            assertParameterListBlob(TgBindParameter.ofBlob("foo", value), TgBindParameter.ofBlob("bar", (byte[]) null), parameter, true);
        }
    }

    @Test
    void testClob() throws Exception {
        {
            var path = createTempFile();
            IceaxeFileUtil.write(path, "abc\ndef\r\nあういえお");
            var clob = TgClob.of(path);
            var parameter = TgBindParameters.of();
            parameter.addClob("foo", clob);
            parameter.addClob("bar", (TgClob) null);

            assertParameterListClob(TgBindParameter.of("foo", clob), TgBindParameter.of("bar", (TgClob) null), parameter, false);
        }
        {
            var path = createTempFile();
            IceaxeFileUtil.write(path, "abc\ndef\r\nあういえお");
            var parameter = TgBindParameters.of();
            parameter.addClob("foo", path);
            parameter.addClob("bar", (Path) null);

            assertParameterListClob(TgBindParameter.ofClob("foo", path), TgBindParameter.ofClob("bar", (Path) null), parameter, false);
        }
        {
            var parameter = TgBindParameters.of();
            parameter.addClob("foo", new StringReader("abc\ndef\r\nあういえお"));
            parameter.addClob("bar", (Reader) null);

            assertParameterListClob(TgBindParameter.ofClob("foo", new StringReader("abc\ndef\r\nあういえお")), TgBindParameter.ofClob("bar", (Reader) null), parameter, true);
        }
        {
            var value = "abc\ndef\r\nあういえお";
            var parameter = TgBindParameters.of();
            parameter.addClob("foo", value);
            parameter.addClob("bar", (String) null);

            assertParameterListClob(TgBindParameter.ofClob("foo", value), TgBindParameter.ofClob("bar", (String) null), parameter, true);
        }
    }

    @Test
    void testAddStringBoolean() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.add("foo", true);

        assertParameterList(TgBindParameter.of("foo", true), parameter);
    }

    @Test
    void testAddStringBooleanWrapper() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.add("foo", Boolean.TRUE);
        parameter.add("bar", (Boolean) null);

        assertParameterList(TgBindParameter.of("foo", Boolean.TRUE), TgBindParameter.of("bar", (Boolean) null), parameter);
    }

    @Test
    void testAddStringInt() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.add("foo", 123);

        assertParameterList(TgBindParameter.of("foo", 123), parameter);
    }

    @Test
    void testAddStringInteger() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.add("foo", Integer.valueOf(123));
        parameter.add("bar", (Integer) null);

        assertParameterList(TgBindParameter.of("foo", 123), TgBindParameter.of("bar", (Integer) null), parameter);
    }

    @Test
    void testAddStringLong() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.add("foo", 123L);

        assertParameterList(TgBindParameter.of("foo", 123L), parameter);
    }

    @Test
    void testAddStringLongWrapper() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.add("foo", Long.valueOf(123));
        parameter.add("bar", (Long) null);

        assertParameterList(TgBindParameter.of("foo", 123L), TgBindParameter.of("bar", (Long) null), parameter);
    }

    @Test
    void testAddStringFloat() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.add("foo", 123f);

        assertParameterList(TgBindParameter.of("foo", 123f), parameter);
    }

    @Test
    void testAddStringFloatWrapper() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.add("foo", Float.valueOf(123));
        parameter.add("bar", (Float) null);

        assertParameterList(TgBindParameter.of("foo", 123f), TgBindParameter.of("bar", (Float) null), parameter);
    }

    @Test
    void testAddStringDouble() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.add("foo", 123d);

        assertParameterList(TgBindParameter.of("foo", 123d), parameter);
    }

    @Test
    void testAddStringDoubleWrapper() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.add("foo", Double.valueOf(123));
        parameter.add("bar", (Double) null);

        assertParameterList(TgBindParameter.of("foo", 123d), TgBindParameter.of("bar", (Double) null), parameter);
    }

    @Test
    void testAddStringBigDecimal() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.add("foo", BigDecimal.valueOf(123));
        parameter.add("bar", (BigDecimal) null);

        assertParameterList(TgBindParameter.of("foo", BigDecimal.valueOf(123)), TgBindParameter.of("bar", (BigDecimal) null), parameter);
    }

    @Test
    void testAddStringBigDecimalScale() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.add("p", new BigDecimal("1.15"), 1);
        parameter.add("m", new BigDecimal("-1.15"), 1);
        parameter.add("bar", (BigDecimal) null, 1);

        var mode = TgBindVariableBigDecimal.DEFAULT_ROUNDING_MODE;
        assertParameterList(TgBindParameter.of("p", new BigDecimal("1.15").setScale(1, mode)), TgBindParameter.of("m", new BigDecimal("-1.15").setScale(1, mode)),
                TgBindParameter.of("bar", (BigDecimal) null), parameter);
    }

    @ParameterizedTest
    @ValueSource(strings = { "FLOOR", "DOWN", "HALF_UP" })
    void testAddStringBigDecimalRoundingMode(RoundingMode mode) throws Exception {
        var parameter = TgBindParameters.of();
        parameter.add("p", new BigDecimal("1.15"), 1, mode);
        parameter.add("m", new BigDecimal("-1.15"), 1, mode);
        parameter.add("bar", (BigDecimal) null, 1, mode);

        assertParameterList(TgBindParameter.of("p", new BigDecimal("1.15").setScale(1, mode)), TgBindParameter.of("m", new BigDecimal("-1.15").setScale(1, mode)),
                TgBindParameter.of("bar", (BigDecimal) null), parameter);
    }

    @Test
    void testAddStringString() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.add("foo", "abc");
        parameter.add("bar", (String) null);

        assertParameterList(TgBindParameter.of("foo", "abc"), TgBindParameter.of("bar", (String) null), parameter);
    }

    @Test
    void testAddStringBytes() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.add("foo", new byte[] { 1, 2, 3 });
        parameter.add("bar", (byte[]) null);

        assertParameterList(TgBindParameter.of("foo", new byte[] { 1, 2, 3 }), TgBindParameter.of("bar", (byte[]) null), parameter);
    }

    @Test
    void testAddStringBooleanArray() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.add("foo", new boolean[] { true, false, true });
        parameter.add("bar", (boolean[]) null);

        assertParameterList(TgBindParameter.of("foo", new boolean[] { true, false, true }), TgBindParameter.of("bar", (boolean[]) null), parameter);
    }

    @Test
    void testAddStringLocalDate() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.add("foo", LocalDate.of(2022, 6, 30));
        parameter.add("bar", (LocalDate) null);

        assertParameterList(TgBindParameter.of("foo", LocalDate.of(2022, 6, 30)), TgBindParameter.of("bar", (LocalDate) null), parameter);
    }

    @Test
    void testAddStringLocalTime() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.add("foo", LocalTime.of(23, 30, 59));
        parameter.add("bar", (LocalTime) null);

        assertParameterList(TgBindParameter.of("foo", LocalTime.of(23, 30, 59)), TgBindParameter.of("bar", (LocalTime) null), parameter);
    }

    @Test
    void testAddStringLocalDateTime() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.add("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59));
        parameter.add("bar", (LocalDateTime) null);

        assertParameterList(TgBindParameter.of("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59)), TgBindParameter.of("bar", (LocalDateTime) null), parameter);
    }

    @Test
    void testAddStringOffsetTime() throws Exception {
        var offset = ZoneOffset.ofHours(9);
        var parameter = TgBindParameters.of();
        parameter.add("foo", OffsetTime.of(23, 30, 59, 0, offset));
        parameter.add("bar", (OffsetTime) null);

        assertParameterList(TgBindParameter.of("foo", OffsetTime.of(23, 30, 59, 0, offset)), TgBindParameter.of("bar", (OffsetTime) null), parameter);
    }

    @Test
    void testAddStringOffsetDateTime() throws Exception {
        var offset = ZoneOffset.ofHours(9);
        var parameter = TgBindParameters.of();
        parameter.add("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset));
        parameter.add("bar", (OffsetDateTime) null);

        assertParameterList(TgBindParameter.of("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset)), TgBindParameter.of("bar", (OffsetDateTime) null), parameter);
    }

    @Test
    void testAddStringZonedDateTime() throws Exception {
        var zdt = ZonedDateTime.of(2022, 6, 30, 23, 30, 59, 999, ZoneId.of("Asia/Tokyo"));
        var parameter = TgBindParameters.of();
        parameter.add("foo", zdt);
        parameter.add("bar", (ZonedDateTime) null);

        assertParameterList(TgBindParameter.of("foo", zdt), TgBindParameter.of("bar", (ZonedDateTime) null), parameter);
    }

    @Test
    void testAddStringBlob() throws Exception {
        var path = createTempFile();
        Files.write(path, new byte[] { 1, 2, 3 });

        var blob = TgBlob.of(path);
        var parameter = TgBindParameters.of();
        parameter.add("foo", blob);
        parameter.add("bar", (TgBlob) null);

        assertParameterListBlob(TgBindParameter.of("foo", blob), TgBindParameter.of("bar", (TgBlob) null), parameter, false);
    }

    @Test
    void testAddStringClob() throws Exception {
        var path = createTempFile();
        IceaxeFileUtil.write(path, "abc\ndef\r\nあいうえお");

        var blob = TgClob.of(path);
        var parameter = TgBindParameters.of();
        parameter.add("foo", blob);
        parameter.add("bar", (TgClob) null);

        assertParameterListClob(TgBindParameter.of("foo", blob), TgBindParameter.of("bar", (TgClob) null), parameter, false);
    }

    @Test
    void testAddStringBlobPath() throws Exception {
        var path = createTempFile();
        Files.write(path, new byte[] { 1, 2, 3 });

        var parameter = TgBindParameters.of();
        parameter.add("foo", TgDataType.BLOB, path);
        parameter.add("bar", TgDataType.BLOB, (Path) null);

        assertParameterListBlob(TgBindParameter.ofBlob("foo", path), TgBindParameter.ofBlob("bar", (Path) null), parameter, false);
    }

    @Test
    void testAddStringClobPath() throws Exception {
        var path = createTempFile();
        IceaxeFileUtil.write(path, "abc\ndef\r\nあいうえお");

        var parameter = TgBindParameters.of();
        parameter.add("foo", TgDataType.CLOB, path);
        parameter.add("bar", TgDataType.CLOB, (Path) null);

        assertParameterListClob(TgBindParameter.ofClob("foo", path), TgBindParameter.ofClob("bar", (Path) null), parameter, false);
    }

    @Test
    void testAddTgParameter() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.add(TgBindParameter.of("foo", 123));
        parameter.add(TgBindParameter.of("bar", "abc"));

        assertParameterList(TgBindParameter.of("foo", 123), TgBindParameter.of("bar", "abc"), parameter);
    }

    @Test
    void testAddTgParameterList() throws Exception {
        var parameter = TgBindParameters.of();
        parameter.add("foo", 123);
        parameter.add("bar", "abc");

        var parameter2 = TgBindParameters.of();
        parameter2.add("zzz1", 123);
        parameter2.add("zzz2", 456);

        parameter.add(parameter2);

        assertParameterList(List.of(TgBindParameter.of("foo", 123), TgBindParameter.of("bar", "abc"), TgBindParameter.of("zzz1", 123), TgBindParameter.of("zzz2", 456)), parameter);
        assertParameterList(TgBindParameter.of("zzz1", 123), TgBindParameter.of("zzz2", 456), parameter2);
    }

    private static Path createTempFile() throws IOException {
        var path = Files.createTempFile("iceaxe-core-test", ".dat");
        path.toFile().deleteOnExit();
        return path;
    }

    private void assertParameterList(TgBindParameter expected, TgBindParameters actual) throws IOException, InterruptedException {
        assertParameterList(List.of(expected), actual);
    }

    private void assertParameterList(TgBindParameter expected1, TgBindParameter expected2, TgBindParameters actual) throws IOException, InterruptedException {
        assertParameterList(List.of(expected1, expected2), actual);
    }

    private void assertParameterList(TgBindParameter expected1, TgBindParameter expected2, TgBindParameter expected3, TgBindParameters actual) throws IOException, InterruptedException {
        assertParameterList(List.of(expected1, expected2, expected3), actual);
    }

    private void assertParameterList(List<TgBindParameter> expected, TgBindParameters actual) throws IOException, InterruptedException {
        var context1Wrapper = new TestLowParameterGenerateContextWrapper();
        var context1 = context1Wrapper.context();
        var expectedLow = expected.stream().map(v -> {
            try {
                return v.toLowParameter(context1);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());

        var context2Wrapper = new TestLowParameterGenerateContextWrapper();
        var context2 = context2Wrapper.context();
        var actualLow = actual.toLowParameterList(context2);

        assertEquals(expectedLow, actualLow);
    }

    private void assertParameterListBlob(TgBindParameter expected1, TgBindParameter expected2, TgBindParameters actual, boolean deleteOnExecuteFinished) throws IOException, InterruptedException {
        var context1Wrapper = new TestLowParameterGenerateContextWrapper();
        var context1 = context1Wrapper.context();
        var expectedLow = List.of(expected1, expected2).stream().map(v -> {
            try {
                return v.toLowParameter(context1);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());

        var context2Wrapper = new TestLowParameterGenerateContextWrapper();
        var context2 = context2Wrapper.context();
        var actualLow = actual.toLowParameterList(context2);

        assertEquals(context1.closeableSet().size(), context2.closeableSet().size());

        assertEquals(expectedLow.size(), actualLow.size());
        for (int i = 0; i < actualLow.size(); i++) {
            var expectLowParameter = expectedLow.get(i);
            var actualLowParameter = actualLow.get(i);
            assertEquals(expectLowParameter.getName(), actualLowParameter.getName());
            // TODO compare blob content
        }
    }

    private void assertParameterListClob(TgBindParameter expected1, TgBindParameter expected2, TgBindParameters actual, boolean deleteOnExecuteFinished) throws IOException, InterruptedException {
        var context1Wrapper = new TestLowParameterGenerateContextWrapper();
        var context1 = context1Wrapper.context();
        var expectedLow = List.of(expected1, expected2).stream().map(v -> {
            try {
                return v.toLowParameter(context1);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());

        var context2Wrapper = new TestLowParameterGenerateContextWrapper();
        var context2 = context2Wrapper.context();
        var actualLow = actual.toLowParameterList(context2);

        assertEquals(context1.closeableSet().size(), context2.closeableSet().size());

        assertEquals(expectedLow.size(), actualLow.size());
        for (int i = 0; i < actualLow.size(); i++) {
            var expectLowParameter = expectedLow.get(i);
            var actualLowParameter = actualLow.get(i);
            assertEquals(expectLowParameter.getName(), actualLowParameter.getName());
            // TODO compare clob content
        }
    }

    @Test
    void testToString() {
        var empty = TgBindParameters.of();
        assertEquals("TgBindParameters[]", empty.toString());
    }
}
