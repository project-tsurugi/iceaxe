package com.tsurugidb.iceaxe.sql.parameter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.math.RoundingMode;
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

import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableBigDecimal;

class TgBindParametersTest {

    @Test
    void testOf() {
        var list = TgBindParameters.of();

        assertParameterList(List.of(), list);
    }

    @Test
    void testOfTgParameterArray() {
        var foo = TgBindVariable.ofInt("foo");
        var list = TgBindParameters.of(foo.bind(123));

        assertParameterList(TgBindParameter.of("foo", 123), list);
    }

    @Test
    void testBoolStringBoolean() {
        var list = TgBindParameters.of();
        list.addBoolean("foo", true);

        assertParameterList(TgBindParameter.of("foo", true), list);
    }

    @Test
    void testBoolStringBooleanWrapper() {
        var list = TgBindParameters.of();
        list.addBoolean("foo", Boolean.TRUE);
        list.addBoolean("bar", null);

        assertParameterList(TgBindParameter.of("foo", Boolean.TRUE), TgBindParameter.of("bar", (Boolean) null), list);
    }

    @Test
    void testIntStringInt() {
        var list = TgBindParameters.of();
        list.addInt("foo", 123);

        assertParameterList(TgBindParameter.of("foo", 123), list);
    }

    @Test
    void testIntStringInteger() {
        var list = TgBindParameters.of();
        list.addInt("foo", Integer.valueOf(123));
        list.addInt("bar", null);

        assertParameterList(TgBindParameter.of("foo", 123), TgBindParameter.of("bar", (Integer) null), list);
    }

    @Test
    void testLongStringLong() {
        var list = TgBindParameters.of();
        list.addLong("foo", 123);

        assertParameterList(TgBindParameter.of("foo", 123L), list);
    }

    @Test
    void testLongStringLongWrapper() {
        var list = TgBindParameters.of();
        list.addLong("foo", Long.valueOf(123));
        list.addLong("bar", null);

        assertParameterList(TgBindParameter.of("foo", 123L), TgBindParameter.of("bar", (Long) null), list);
    }

    @Test
    void testFloatStringFloat() {
        var list = TgBindParameters.of();
        list.addFloat("foo", 123);

        assertParameterList(TgBindParameter.of("foo", 123f), list);
    }

    @Test
    void testFloatStringFloatWrapper() {
        var list = TgBindParameters.of();
        list.addFloat("foo", Float.valueOf(123));
        list.addFloat("bar", null);

        assertParameterList(TgBindParameter.of("foo", 123f), TgBindParameter.of("bar", (Float) null), list);
    }

    @Test
    void testDoubleStringDouble() {
        var list = TgBindParameters.of();
        list.addDouble("foo", 123);

        assertParameterList(TgBindParameter.of("foo", 123d), list);
    }

    @Test
    void testDoubleStringDoubleWrapper() {
        var list = TgBindParameters.of();
        list.addDouble("foo", Double.valueOf(123));
        list.addDouble("bar", null);

        assertParameterList(TgBindParameter.of("foo", 123d), TgBindParameter.of("bar", (Double) null), list);
    }

    @Test
    void testDecimal() {
        var list = TgBindParameters.of();
        list.addDecimal("foo", BigDecimal.valueOf(123));
        list.addDecimal("bar", null);

        assertParameterList(TgBindParameter.of("foo", BigDecimal.valueOf(123)), TgBindParameter.of("bar", (BigDecimal) null), list);
    }

    @Test
    void testDecimalScale() {
        var list = TgBindParameters.of();
        list.addDecimal("p", new BigDecimal("1.01"), 1);
        list.addDecimal("m", new BigDecimal("-1.01"), 1);
        list.addDecimal("bar", null, 1);

        var mode = TgBindVariableBigDecimal.DEFAULT_ROUNDING_MODE;
        assertParameterList(TgBindParameter.of("p", new BigDecimal("1.01").setScale(1, mode)), TgBindParameter.of("m", new BigDecimal("-1.01").setScale(1, mode)), TgBindParameter.of("bar", (BigDecimal) null),
                list);
    }

    @ParameterizedTest
    @ValueSource(strings = { "FLOOR", "DOWN", "HALF_UP" })
    void testDecimalRoundingMode(RoundingMode mode) {
        var list = TgBindParameters.of();
        list.addDecimal("p", new BigDecimal("1.05"), 1, mode);
        list.addDecimal("m", new BigDecimal("-1.05"), 1, mode);
        list.addDecimal("bar", null, 1, mode);

        assertParameterList(TgBindParameter.of("p", new BigDecimal("1.05").setScale(1, mode)), TgBindParameter.of("m", new BigDecimal("-1.05").setScale(1, mode)), TgBindParameter.of("bar", (BigDecimal) null),
                list);
    }

    @Test
    void testString() {
        var list = TgBindParameters.of();
        list.addString("foo", "abc");
        list.addString("bar", null);

        assertParameterList(TgBindParameter.of("foo", "abc"), TgBindParameter.of("bar", (String) null), list);
    }

    @Test
    void testBytes() {
        var list = TgBindParameters.of();
        list.addBytes("foo", new byte[] { 1, 2, 3 });
        list.addBytes("bar", null);

        assertParameterList(TgBindParameter.of("foo", new byte[] { 1, 2, 3 }), TgBindParameter.of("bar", (byte[]) null), list);
    }

    @Test
    void testBits() {
        var list = TgBindParameters.of();
        list.addBits("foo", new boolean[] { true, false, true });
        list.addBits("bar", null);

        assertParameterList(TgBindParameter.of("foo", new boolean[] { true, false, true }), TgBindParameter.of("bar", (boolean[]) null), list);
    }

    @Test
    void testDate() {
        var list = TgBindParameters.of();
        list.addDate("foo", LocalDate.of(2022, 6, 30));
        list.addDate("bar", null);

        assertParameterList(TgBindParameter.of("foo", LocalDate.of(2022, 6, 30)), TgBindParameter.of("bar", (LocalDate) null), list);
    }

    @Test
    void testTime() {
        var list = TgBindParameters.of();
        list.addTime("foo", LocalTime.of(23, 30, 59));
        list.addTime("bar", null);

        assertParameterList(TgBindParameter.of("foo", LocalTime.of(23, 30, 59)), TgBindParameter.of("bar", (LocalTime) null), list);
    }

    @Test
    void testDateTime() {
        var list = TgBindParameters.of();
        list.addDateTime("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59));
        list.addDateTime("bar", null);

        assertParameterList(TgBindParameter.of("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59)), TgBindParameter.of("bar", (LocalDateTime) null), list);
    }

    @Test
    void testOffsetTime() {
        var offset = ZoneOffset.ofHours(9);
        var list = TgBindParameters.of();
        list.addOffsetTime("foo", OffsetTime.of(23, 30, 59, 0, offset));
        list.addOffsetTime("bar", null);

        assertParameterList(TgBindParameter.of("foo", OffsetTime.of(23, 30, 59, 0, offset)), TgBindParameter.of("bar", (OffsetTime) null), list);
    }

    @Test
    void testOffsetDateTime() {
        var offset = ZoneOffset.ofHours(9);
        var list = TgBindParameters.of();
        list.addOffsetDateTime("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset));
        list.addOffsetDateTime("bar", null);

        assertParameterList(TgBindParameter.of("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset)), TgBindParameter.of("bar", (OffsetTime) null), list);
    }

    @Test
    void testZonedDateTime() {
        var zdt = ZonedDateTime.of(2022, 6, 30, 23, 30, 59, 999, ZoneId.of("Asia/Tokyo"));
        var list = TgBindParameters.of();
        list.addZonedDateTime("foo", zdt);
        list.addZonedDateTime("bar", null);

        assertParameterList(TgBindParameter.of("foo", zdt), TgBindParameter.of("bar", (ZonedDateTime) null), list);
    }

    @Test
    void testAddStringBoolean() {
        var list = TgBindParameters.of();
        list.add("foo", true);

        assertParameterList(TgBindParameter.of("foo", true), list);
    }

    @Test
    void testAddStringBooleanWrapper() {
        var list = TgBindParameters.of();
        list.add("foo", Boolean.TRUE);
        list.add("bar", (Boolean) null);

        assertParameterList(TgBindParameter.of("foo", Boolean.TRUE), TgBindParameter.of("bar", (Boolean) null), list);
    }

    @Test
    void testAddStringInt() {
        var list = TgBindParameters.of();
        list.add("foo", 123);

        assertParameterList(TgBindParameter.of("foo", 123), list);
    }

    @Test
    void testAddStringInteger() {
        var list = TgBindParameters.of();
        list.add("foo", Integer.valueOf(123));
        list.add("bar", (Integer) null);

        assertParameterList(TgBindParameter.of("foo", 123), TgBindParameter.of("bar", (Integer) null), list);
    }

    @Test
    void testAddStringLong() {
        var list = TgBindParameters.of();
        list.add("foo", 123L);

        assertParameterList(TgBindParameter.of("foo", 123L), list);
    }

    @Test
    void testAddStringLongWrapoper() {
        var list = TgBindParameters.of();
        list.add("foo", Long.valueOf(123));
        list.add("bar", (Long) null);

        assertParameterList(TgBindParameter.of("foo", 123L), TgBindParameter.of("bar", (Long) null), list);
    }

    @Test
    void testAddStringFloat() {
        var list = TgBindParameters.of();
        list.add("foo", 123f);

        assertParameterList(TgBindParameter.of("foo", 123f), list);
    }

    @Test
    void testAddStringFloatWrapper() {
        var list = TgBindParameters.of();
        list.add("foo", Float.valueOf(123));
        list.add("bar", (Float) null);

        assertParameterList(TgBindParameter.of("foo", 123f), TgBindParameter.of("bar", (Float) null), list);
    }

    @Test
    void testAddStringDouble() {
        var list = TgBindParameters.of();
        list.add("foo", 123d);

        assertParameterList(TgBindParameter.of("foo", 123d), list);
    }

    @Test
    void testAddStringDoubleWrapper() {
        var list = TgBindParameters.of();
        list.add("foo", Double.valueOf(123));
        list.add("bar", (Double) null);

        assertParameterList(TgBindParameter.of("foo", 123d), TgBindParameter.of("bar", (Double) null), list);
    }

    @Test
    void testAddStringBigDecimal() {
        var list = TgBindParameters.of();
        list.add("foo", BigDecimal.valueOf(123));
        list.add("bar", (BigDecimal) null);

        assertParameterList(TgBindParameter.of("foo", BigDecimal.valueOf(123)), TgBindParameter.of("bar", (BigDecimal) null), list);
    }

    @Test
    void testAddStringBigDecimalScale() {
        var list = TgBindParameters.of();
        list.add("p", new BigDecimal("1.15"), 1);
        list.add("m", new BigDecimal("-1.15"), 1);
        list.add("bar", (BigDecimal) null, 1);

        var mode = TgBindVariableBigDecimal.DEFAULT_ROUNDING_MODE;
        assertParameterList(TgBindParameter.of("p", new BigDecimal("1.15").setScale(1, mode)), TgBindParameter.of("m", new BigDecimal("-1.15").setScale(1, mode)), TgBindParameter.of("bar", (BigDecimal) null),
                list);
    }

    @ParameterizedTest
    @ValueSource(strings = { "FLOOR", "DOWN", "HALF_UP" })
    void testAddStringBigDecimalRoundingMode(RoundingMode mode) {
        var list = TgBindParameters.of();
        list.add("p", new BigDecimal("1.15"), 1, mode);
        list.add("m", new BigDecimal("-1.15"), 1, mode);
        list.add("bar", (BigDecimal) null, 1, mode);

        assertParameterList(TgBindParameter.of("p", new BigDecimal("1.15").setScale(1, mode)), TgBindParameter.of("m", new BigDecimal("-1.15").setScale(1, mode)), TgBindParameter.of("bar", (BigDecimal) null),
                list);
    }

    @Test
    void testAddStringString() {
        var list = TgBindParameters.of();
        list.add("foo", "abc");
        list.add("bar", (String) null);

        assertParameterList(TgBindParameter.of("foo", "abc"), TgBindParameter.of("bar", (String) null), list);
    }

    @Test
    void testAddStringByteArray() {
        var list = TgBindParameters.of();
        list.add("foo", new byte[] { 1, 2, 3 });
        list.add("bar", (byte[]) null);

        assertParameterList(TgBindParameter.of("foo", new byte[] { 1, 2, 3 }), TgBindParameter.of("bar", (byte[]) null), list);
    }

    @Test
    void testAddStringBooleanArray() {
        var list = TgBindParameters.of();
        list.add("foo", new boolean[] { true, false, true });
        list.add("bar", (boolean[]) null);

        assertParameterList(TgBindParameter.of("foo", new boolean[] { true, false, true }), TgBindParameter.of("bar", (boolean[]) null), list);
    }

    @Test
    void testAddStringLocalDate() {
        var list = TgBindParameters.of();
        list.add("foo", LocalDate.of(2022, 6, 30));
        list.add("bar", (LocalDate) null);

        assertParameterList(TgBindParameter.of("foo", LocalDate.of(2022, 6, 30)), TgBindParameter.of("bar", (LocalDate) null), list);
    }

    @Test
    void testAddStringLocalTime() {
        var list = TgBindParameters.of();
        list.add("foo", LocalTime.of(23, 30, 59));
        list.add("bar", (LocalTime) null);

        assertParameterList(TgBindParameter.of("foo", LocalTime.of(23, 30, 59)), TgBindParameter.of("bar", (LocalTime) null), list);
    }

    @Test
    void testAddStringLocalDateTime() {
        var list = TgBindParameters.of();
        list.add("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59));
        list.add("bar", (LocalDateTime) null);

        assertParameterList(TgBindParameter.of("foo", LocalDateTime.of(2022, 9, 22, 23, 30, 59)), TgBindParameter.of("bar", (LocalDateTime) null), list);
    }

    @Test
    void testAddStringOffsetTime() {
        var offset = ZoneOffset.ofHours(9);
        var list = TgBindParameters.of();
        list.add("foo", OffsetTime.of(23, 30, 59, 0, offset));
        list.add("bar", (OffsetTime) null);

        assertParameterList(TgBindParameter.of("foo", OffsetTime.of(23, 30, 59, 0, offset)), TgBindParameter.of("bar", (OffsetTime) null), list);
    }

    @Test
    void testAddStringOffsetDateTime() {
        var offset = ZoneOffset.ofHours(9);
        var list = TgBindParameters.of();
        list.add("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset));
        list.add("bar", (OffsetDateTime) null);

        assertParameterList(TgBindParameter.of("foo", OffsetDateTime.of(2022, 9, 22, 23, 30, 59, 0, offset)), TgBindParameter.of("bar", (OffsetDateTime) null), list);
    }

    @Test
    void testAddStringZonedDateTime() {
        var zdt = ZonedDateTime.of(2022, 6, 30, 23, 30, 59, 999, ZoneId.of("Asia/Tokyo"));
        var list = TgBindParameters.of();
        list.add("foo", zdt);
        list.add("bar", (ZonedDateTime) null);

        assertParameterList(TgBindParameter.of("foo", zdt), TgBindParameter.of("bar", (ZonedDateTime) null), list);
    }

    @Test
    void testAddTgParameter() {
        var list = TgBindParameters.of();
        list.add(TgBindParameter.of("foo", 123));
        list.add(TgBindParameter.of("bar", "abc"));

        assertParameterList(TgBindParameter.of("foo", 123), TgBindParameter.of("bar", "abc"), list);
    }

    @Test
    void testAddTgParameterList() {
        var list = TgBindParameters.of();
        list.add("foo", 123);
        list.add("bar", "abc");

        var list2 = TgBindParameters.of();
        list2.add("zzz1", 123);
        list2.add("zzz2", 456);

        list.add(list2);

        assertParameterList(List.of(TgBindParameter.of("foo", 123), TgBindParameter.of("bar", "abc"), TgBindParameter.of("zzz1", 123), TgBindParameter.of("zzz2", 456)), list);
        assertParameterList(TgBindParameter.of("zzz1", 123), TgBindParameter.of("zzz2", 456), list2);
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
        var expectedLow = expected.stream().map(TgBindParameter::toLowParameter).collect(Collectors.toList());
        var actualLow = actual.toLowParameterList();
        assertEquals(expectedLow, actualLow);
    }

    @Test
    void testToString() {
        var empty = TgBindParameters.of();
        assertEquals("TgBindParameters[]", empty.toString());
    }
}
