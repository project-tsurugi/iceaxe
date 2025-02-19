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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
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
import com.tsurugidb.iceaxe.test.TestBlobUtil;
import com.tsurugidb.iceaxe.util.IceaxeCloseableSet;

class TgBindParametersTest {

    @Test
    void testOf() {
        var parameter = TgBindParameters.of();

        assertParameterList(List.of(), parameter);
    }

    @Test
    void testOfArray() {
        var foo = TgBindVariable.ofInt("foo");
        var parameter = TgBindParameters.of(foo.bind(123));

        assertParameterList(TgBindParameter.of("foo", 123), parameter);
    }

    @Test
    void testOfCollection() {
        var foo = TgBindVariable.ofInt("foo");
        var list = List.of(foo.bind(123));
        var parameter = TgBindParameters.of(list);

        assertParameterList(TgBindParameter.of("foo", 123), parameter);
    }

    @Test
    void testBoolStringBoolean() {
        var parameter = TgBindParameters.of();
        parameter.addBoolean("foo", true);

        assertParameterList(TgBindParameter.of("foo", true), parameter);
    }

    @Test
    void testBoolStringBooleanWrapper() {
        var parameter = TgBindParameters.of();
        parameter.addBoolean("foo", Boolean.TRUE);
        parameter.addBoolean("bar", null);

        assertParameterList(TgBindParameter.of("foo", Boolean.TRUE), TgBindParameter.of("bar", (Boolean) null), parameter);
    }

    @Test
    void testIntStringInt() {
        var parameter = TgBindParameters.of();
        parameter.addInt("foo", 123);

        assertParameterList(TgBindParameter.of("foo", 123), parameter);
    }

    @Test
    void testIntStringInteger() {
        var parameter = TgBindParameters.of();
        parameter.addInt("foo", Integer.valueOf(123));
        parameter.addInt("bar", null);

        assertParameterList(TgBindParameter.of("foo", 123), TgBindParameter.of("bar", (Integer) null), parameter);
    }

    @Test
    void testLongStringLong() {
        var parameter = TgBindParameters.of();
        parameter.addLong("foo", 123);

        assertParameterList(TgBindParameter.of("foo", 123L), parameter);
    }

    @Test
    void testLongStringLongWrapper() {
        var parameter = TgBindParameters.of();
        parameter.addLong("foo", Long.valueOf(123));
        parameter.addLong("bar", null);

        assertParameterList(TgBindParameter.of("foo", 123L), TgBindParameter.of("bar", (Long) null), parameter);
    }

    @Test
    void testFloatStringFloat() {
        var parameter = TgBindParameters.of();
        parameter.addFloat("foo", 123);

        assertParameterList(TgBindParameter.of("foo", 123f), parameter);
    }

    @Test
    void testFloatStringFloatWrapper() {
        var parameter = TgBindParameters.of();
        parameter.addFloat("foo", Float.valueOf(123));
        parameter.addFloat("bar", null);

        assertParameterList(TgBindParameter.of("foo", 123f), TgBindParameter.of("bar", (Float) null), parameter);
    }

    @Test
    void testDoubleStringDouble() {
        var parameter = TgBindParameters.of();
        parameter.addDouble("foo", 123);

        assertParameterList(TgBindParameter.of("foo", 123d), parameter);
    }

    @Test
    void testDoubleStringDoubleWrapper() {
        var parameter = TgBindParameters.of();
        parameter.addDouble("foo", Double.valueOf(123));
        parameter.addDouble("bar", null);

        assertParameterList(TgBindParameter.of("foo", 123d), TgBindParameter.of("bar", (Double) null), parameter);
    }

    @Test
    void testDecimal() {
        var parameter = TgBindParameters.of();
        parameter.addDecimal("foo", BigDecimal.valueOf(123));
        parameter.addDecimal("bar", null);

        assertParameterList(TgBindParameter.of("foo", BigDecimal.valueOf(123)), TgBindParameter.of("bar", (BigDecimal) null), parameter);
    }

    @Test
    void testDecimalScale() {
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
    void testDecimalRoundingMode(RoundingMode mode) {
        var parameter = TgBindParameters.of();
        parameter.addDecimal("p", new BigDecimal("1.05"), 1, mode);
        parameter.addDecimal("m", new BigDecimal("-1.05"), 1, mode);
        parameter.addDecimal("bar", null, 1, mode);

        assertParameterList(TgBindParameter.of("p", new BigDecimal("1.05").setScale(1, mode)), TgBindParameter.of("m", new BigDecimal("-1.05").setScale(1, mode)),
                TgBindParameter.of("bar", (BigDecimal) null), parameter);
    }

    @Test
    void testString() {
        var parameter = TgBindParameters.of();
        parameter.addString("foo", "abc");
        parameter.addString("bar", null);

        assertParameterList(TgBindParameter.of("foo", "abc"), TgBindParameter.of("bar", (String) null), parameter);
    }

    @Test
    void testBytes() {
        var parameter = TgBindParameters.of();
        parameter.addBytes("foo", new byte[] { 1, 2, 3 });
        parameter.addBytes("bar", null);

        assertParameterList(TgBindParameter.of("foo", new byte[] { 1, 2, 3 }), TgBindParameter.of("bar", (byte[]) null), parameter);
    }

    @Test
    void testBits() {
        var parameter = TgBindParameters.of();
        parameter.addBits("foo", new boolean[] { true, false, true });
        parameter.addBits("bar", null);

        assertParameterList(TgBindParameter.of("foo", new boolean[] { true, false, true }), TgBindParameter.of("bar", (boolean[]) null), parameter);
    }

    @Test
    void testDate() {
        var parameter = TgBindParameters.of();
        parameter.addDate("foo", LocalDate.of(2022, 6, 30));
        parameter.addDate("bar", null);

        assertParameterList(TgBindParameter.of("foo", LocalDate.of(2022, 6, 30)), TgBindParameter.of("bar", (LocalDate) null), parameter);
    }

    @Test
    void testTime() {
        var parameter = TgBindParameters.of();
        parameter.addTime("foo", LocalTime.of(23, 30, 59));
        parameter.addTime("bar", null);

        assertParameterList(TgBindParameter.of("foo", LocalTime.of(23, 30, 59)), TgBindParameter.of("bar", (LocalTime) null), parameter);
    }

    @Test
    void testDateTime() {
        var parameter = TgBindParameters.of();
        parameter.addDateTime("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59));
        parameter.addDateTime("bar", null);

        assertParameterList(TgBindParameter.of("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59)), TgBindParameter.of("bar", (LocalDateTime) null), parameter);
    }

    @Test
    void testOffsetTime() {
        var offset = ZoneOffset.ofHours(9);
        var parameter = TgBindParameters.of();
        parameter.addOffsetTime("foo", OffsetTime.of(23, 30, 59, 0, offset));
        parameter.addOffsetTime("bar", null);

        assertParameterList(TgBindParameter.of("foo", OffsetTime.of(23, 30, 59, 0, offset)), TgBindParameter.of("bar", (OffsetTime) null), parameter);
    }

    @Test
    void testOffsetDateTime() {
        var offset = ZoneOffset.ofHours(9);
        var parameter = TgBindParameters.of();
        parameter.addOffsetDateTime("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset));
        parameter.addOffsetDateTime("bar", null);

        assertParameterList(TgBindParameter.of("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset)), TgBindParameter.of("bar", (OffsetTime) null), parameter);
    }

    @Test
    void testZonedDateTime() {
        var zdt = ZonedDateTime.of(2022, 6, 30, 23, 30, 59, 999, ZoneId.of("Asia/Tokyo"));
        var parameter = TgBindParameters.of();
        parameter.addZonedDateTime("foo", zdt);
        parameter.addZonedDateTime("bar", null);

        assertParameterList(TgBindParameter.of("foo", zdt), TgBindParameter.of("bar", (ZonedDateTime) null), parameter);
    }

    @Test
    void testBlob() throws Exception {
        {
            var path = Files.createTempFile("iceaxe-core-test", ".dat");
            Files.write(path, new byte[] { 1, 2, 3 });
            var blob = TgBlob.of(path);
            var parameter = TgBindParameters.of();
            parameter.addBlob("foo", blob);
            parameter.addBlob("bar", (TgBlob) null);

            assertParameterListBlob(TgBindParameter.of("foo", blob), TgBindParameter.of("bar", (TgBlob) null), parameter, false);
        }
        {
            var path = Files.createTempFile("iceaxe-core-test", ".dat");
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

    // TODO CLOB

    @Test
    void testAddStringBoolean() {
        var parameter = TgBindParameters.of();
        parameter.add("foo", true);

        assertParameterList(TgBindParameter.of("foo", true), parameter);
    }

    @Test
    void testAddStringBooleanWrapper() {
        var parameter = TgBindParameters.of();
        parameter.add("foo", Boolean.TRUE);
        parameter.add("bar", (Boolean) null);

        assertParameterList(TgBindParameter.of("foo", Boolean.TRUE), TgBindParameter.of("bar", (Boolean) null), parameter);
    }

    @Test
    void testAddStringInt() {
        var parameter = TgBindParameters.of();
        parameter.add("foo", 123);

        assertParameterList(TgBindParameter.of("foo", 123), parameter);
    }

    @Test
    void testAddStringInteger() {
        var parameter = TgBindParameters.of();
        parameter.add("foo", Integer.valueOf(123));
        parameter.add("bar", (Integer) null);

        assertParameterList(TgBindParameter.of("foo", 123), TgBindParameter.of("bar", (Integer) null), parameter);
    }

    @Test
    void testAddStringLong() {
        var parameter = TgBindParameters.of();
        parameter.add("foo", 123L);

        assertParameterList(TgBindParameter.of("foo", 123L), parameter);
    }

    @Test
    void testAddStringLongWrapper() {
        var parameter = TgBindParameters.of();
        parameter.add("foo", Long.valueOf(123));
        parameter.add("bar", (Long) null);

        assertParameterList(TgBindParameter.of("foo", 123L), TgBindParameter.of("bar", (Long) null), parameter);
    }

    @Test
    void testAddStringFloat() {
        var parameter = TgBindParameters.of();
        parameter.add("foo", 123f);

        assertParameterList(TgBindParameter.of("foo", 123f), parameter);
    }

    @Test
    void testAddStringFloatWrapper() {
        var parameter = TgBindParameters.of();
        parameter.add("foo", Float.valueOf(123));
        parameter.add("bar", (Float) null);

        assertParameterList(TgBindParameter.of("foo", 123f), TgBindParameter.of("bar", (Float) null), parameter);
    }

    @Test
    void testAddStringDouble() {
        var parameter = TgBindParameters.of();
        parameter.add("foo", 123d);

        assertParameterList(TgBindParameter.of("foo", 123d), parameter);
    }

    @Test
    void testAddStringDoubleWrapper() {
        var parameter = TgBindParameters.of();
        parameter.add("foo", Double.valueOf(123));
        parameter.add("bar", (Double) null);

        assertParameterList(TgBindParameter.of("foo", 123d), TgBindParameter.of("bar", (Double) null), parameter);
    }

    @Test
    void testAddStringBigDecimal() {
        var parameter = TgBindParameters.of();
        parameter.add("foo", BigDecimal.valueOf(123));
        parameter.add("bar", (BigDecimal) null);

        assertParameterList(TgBindParameter.of("foo", BigDecimal.valueOf(123)), TgBindParameter.of("bar", (BigDecimal) null), parameter);
    }

    @Test
    void testAddStringBigDecimalScale() {
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
    void testAddStringBigDecimalRoundingMode(RoundingMode mode) {
        var parameter = TgBindParameters.of();
        parameter.add("p", new BigDecimal("1.15"), 1, mode);
        parameter.add("m", new BigDecimal("-1.15"), 1, mode);
        parameter.add("bar", (BigDecimal) null, 1, mode);

        assertParameterList(TgBindParameter.of("p", new BigDecimal("1.15").setScale(1, mode)), TgBindParameter.of("m", new BigDecimal("-1.15").setScale(1, mode)),
                TgBindParameter.of("bar", (BigDecimal) null), parameter);
    }

    @Test
    void testAddStringString() {
        var parameter = TgBindParameters.of();
        parameter.add("foo", "abc");
        parameter.add("bar", (String) null);

        assertParameterList(TgBindParameter.of("foo", "abc"), TgBindParameter.of("bar", (String) null), parameter);
    }

    @Test
    void testAddStringBytes() {
        var parameter = TgBindParameters.of();
        parameter.add("foo", new byte[] { 1, 2, 3 });
        parameter.add("bar", (byte[]) null);

        assertParameterList(TgBindParameter.of("foo", new byte[] { 1, 2, 3 }), TgBindParameter.of("bar", (byte[]) null), parameter);
    }

    @Test
    void testAddStringBooleanArray() {
        var parameter = TgBindParameters.of();
        parameter.add("foo", new boolean[] { true, false, true });
        parameter.add("bar", (boolean[]) null);

        assertParameterList(TgBindParameter.of("foo", new boolean[] { true, false, true }), TgBindParameter.of("bar", (boolean[]) null), parameter);
    }

    @Test
    void testAddStringLocalDate() {
        var parameter = TgBindParameters.of();
        parameter.add("foo", LocalDate.of(2022, 6, 30));
        parameter.add("bar", (LocalDate) null);

        assertParameterList(TgBindParameter.of("foo", LocalDate.of(2022, 6, 30)), TgBindParameter.of("bar", (LocalDate) null), parameter);
    }

    @Test
    void testAddStringLocalTime() {
        var parameter = TgBindParameters.of();
        parameter.add("foo", LocalTime.of(23, 30, 59));
        parameter.add("bar", (LocalTime) null);

        assertParameterList(TgBindParameter.of("foo", LocalTime.of(23, 30, 59)), TgBindParameter.of("bar", (LocalTime) null), parameter);
    }

    @Test
    void testAddStringLocalDateTime() {
        var parameter = TgBindParameters.of();
        parameter.add("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59));
        parameter.add("bar", (LocalDateTime) null);

        assertParameterList(TgBindParameter.of("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59)), TgBindParameter.of("bar", (LocalDateTime) null), parameter);
    }

    @Test
    void testAddStringOffsetTime() {
        var offset = ZoneOffset.ofHours(9);
        var parameter = TgBindParameters.of();
        parameter.add("foo", OffsetTime.of(23, 30, 59, 0, offset));
        parameter.add("bar", (OffsetTime) null);

        assertParameterList(TgBindParameter.of("foo", OffsetTime.of(23, 30, 59, 0, offset)), TgBindParameter.of("bar", (OffsetTime) null), parameter);
    }

    @Test
    void testAddStringOffsetDateTime() {
        var offset = ZoneOffset.ofHours(9);
        var parameter = TgBindParameters.of();
        parameter.add("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset));
        parameter.add("bar", (OffsetDateTime) null);

        assertParameterList(TgBindParameter.of("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset)), TgBindParameter.of("bar", (OffsetDateTime) null), parameter);
    }

    @Test
    void testAddStringZonedDateTime() {
        var zdt = ZonedDateTime.of(2022, 6, 30, 23, 30, 59, 999, ZoneId.of("Asia/Tokyo"));
        var parameter = TgBindParameters.of();
        parameter.add("foo", zdt);
        parameter.add("bar", (ZonedDateTime) null);

        assertParameterList(TgBindParameter.of("foo", zdt), TgBindParameter.of("bar", (ZonedDateTime) null), parameter);
    }

    @Test
    void testAddStringBlob() throws Exception {
        var path = Files.createTempFile("iceaxe-core-test", ".dat");
        Files.write(path, new byte[] { 1, 2, 3 });

        var blob = TgBlob.of(path);
        var parameter = TgBindParameters.of();
        parameter.add("foo", blob);
        parameter.add("bar", (TgBlob) null);

        assertParameterListBlob(TgBindParameter.of("foo", blob), TgBindParameter.of("bar", (TgBlob) null), parameter, false);
    }

    // TODO CLOB

    @Test
    void testAddStringPath() throws Exception {
        var path = Files.createTempFile("iceaxe-core-test", ".dat");
        Files.write(path, new byte[] { 1, 2, 3 });

        var parameter = TgBindParameters.of();
        parameter.add("foo", TgDataType.BLOB, path);
        parameter.add("bar", TgDataType.BLOB, (Path) null);

        assertParameterListBlob(TgBindParameter.ofBlob("foo", path), TgBindParameter.ofBlob("bar", (Path) null), parameter, false);

        // TODO CLOB
    }

    @Test
    void testAddTgParameter() {
        var parameter = TgBindParameters.of();
        parameter.add(TgBindParameter.of("foo", 123));
        parameter.add(TgBindParameter.of("bar", "abc"));

        assertParameterList(TgBindParameter.of("foo", 123), TgBindParameter.of("bar", "abc"), parameter);
    }

    @Test
    void testAddTgParameterList() {
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

    private void assertParameterList(TgBindParameter expected, TgBindParameters actual) {
        assertParameterList(List.of(expected), actual);
    }

    private void assertParameterList(TgBindParameter expected1, TgBindParameter expected2, TgBindParameters actual) {
        assertParameterList(List.of(expected1, expected2), actual);
    }

    private void assertParameterList(TgBindParameter expected1, TgBindParameter expected2, TgBindParameter expected3, TgBindParameters actual) {
        assertParameterList(List.of(expected1, expected2, expected3), actual);
    }

    private void assertParameterList(List<TgBindParameter> expected, TgBindParameters actual) {
        IceaxeCloseableSet closeableSet1 = new IceaxeCloseableSet();
        var expectedLow = expected.stream().map(v -> v.toLowParameter(closeableSet1)).collect(Collectors.toList());
        IceaxeCloseableSet closeableSet2 = new IceaxeCloseableSet();
        var actualLow = actual.toLowParameterList(closeableSet2);

        assertEquals(expectedLow, actualLow);
    }

    private void assertParameterListBlob(TgBindParameter expected1, TgBindParameter expected2, TgBindParameters actual, boolean deleteOnExecuteFinished) {
        IceaxeCloseableSet closeableSet1 = new IceaxeCloseableSet();
        var expectedLow = List.of(expected1, expected2).stream().map(v -> v.toLowParameter(closeableSet1)).collect(Collectors.toList());
        IceaxeCloseableSet closeableSet2 = new IceaxeCloseableSet();
        var actualLow = actual.toLowParameterList(closeableSet2);

        if (deleteOnExecuteFinished) {
            var list = TestBlobUtil.getBlobList(closeableSet2);
            assertEquals(1, list.size());
            for (var blob : list) {
                assertTrue(blob.isDeleteOnExecuteFinished());
            }
        } else {
            assertEquals(0, closeableSet2.size());
        }

        assertEquals(expectedLow.size(), actualLow.size());
        for (int i = 0; i < actualLow.size(); i++) {
            var expectLowParameter = expectedLow.get(i);
            var actualLowParameter = actualLow.get(i);
            assertEquals(expectLowParameter.getName(), actualLowParameter.getName());

            var expectedPathStr = expectLowParameter.getBlob().getLocalPath();
            var actualPathStr = actualLowParameter.getBlob().getLocalPath();

            if (expectedPathStr.isEmpty()) {
                assertEquals("", actualPathStr);
                return;
            }

            var expectedPath = Path.of(expectedPathStr);
            var actualPath = Path.of(actualPathStr);

            try {
                var expectedValue = Files.readAllBytes(expectedPath);
                var actualValue = Files.readAllBytes(actualPath);
                assertArrayEquals(expectedValue, actualValue);

                Files.deleteIfExists(expectedPath);
                Files.deleteIfExists(actualPath);
            } catch (IOException ie) {
                throw new UncheckedIOException(ie.getMessage(), ie);
            }
        }
    }

    @Test
    void testToString() {
        var empty = TgBindParameters.of();
        assertEquals("TgBindParameters[]", empty.toString());
    }
}
