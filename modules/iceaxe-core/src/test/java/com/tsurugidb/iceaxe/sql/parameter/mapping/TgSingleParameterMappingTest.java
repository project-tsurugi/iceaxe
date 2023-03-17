package com.tsurugidb.iceaxe.sql.parameter.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.tsubakuro.sql.Parameters;

class TgSingleParameterMappingTest {

    @Test
    void testOfBoolean() {
        var mapping = TgSingleParameterMapping.ofBoolean("foo");

        var type = TgDataType.BOOLEAN;
        assertMapping(type, null, mapping);
        assertMapping(type, true, mapping);
    }

    @Test
    void testOfInt() {
        var mapping = TgSingleParameterMapping.ofInt("foo");

        var type = TgDataType.INT;
        assertMapping(type, null, mapping);
        assertMapping(type, 123, mapping);
    }

    @Test
    void testOfLong() {
        var mapping = TgSingleParameterMapping.ofLong("foo");

        var type = TgDataType.LONG;
        assertMapping(type, null, mapping);
        assertMapping(type, 123L, mapping);
    }

    @Test
    void testOfFloat() {
        var mapping = TgSingleParameterMapping.ofFloat("foo");

        var type = TgDataType.FLOAT;
        assertMapping(type, null, mapping);
        assertMapping(type, 123f, mapping);
    }

    @Test
    void testOfDouble() {
        var mapping = TgSingleParameterMapping.ofDouble("foo");

        var type = TgDataType.DOUBLE;
        assertMapping(type, null, mapping);
        assertMapping(type, 123d, mapping);
    }

    @Test
    void testOfDecimal() {
        var mapping = TgSingleParameterMapping.ofDecimal("foo");

        var type = TgDataType.DECIMAL;
        assertMapping(type, null, mapping);
        assertMapping(type, BigDecimal.valueOf(123), mapping);
    }

    @Test
    void testOfDecimalInt() {
        var mapping = TgSingleParameterMapping.ofDecimal("foo", 2);

        var type = TgDataType.DECIMAL;
        assertMapping(type, null, mapping);
        assertMapping(type, BigDecimal.valueOf(123.456), BigDecimal.valueOf(123.45), mapping);
    }

    @Test
    void testOfDecimalIntRoundingMode() {
        var mapping = TgSingleParameterMapping.ofDecimal("foo", 2, RoundingMode.HALF_UP);

        var type = TgDataType.DECIMAL;
        assertMapping(type, null, mapping);
        assertMapping(type, BigDecimal.valueOf(123.456), BigDecimal.valueOf(123.46), mapping);
    }

    @Test
    void testOfString() {
        var mapping = TgSingleParameterMapping.ofString("foo");

        var type = TgDataType.STRING;
        assertMapping(type, null, mapping);
        assertMapping(type, "abc", mapping);
    }

    @Test
    void testOfBytes() {
        var mapping = TgSingleParameterMapping.ofBytes("foo");

        var type = TgDataType.BYTES;
        assertMapping(type, null, mapping);
        assertMapping(type, new byte[] { 1, 2, 3 }, mapping);
    }

    @Test
    void testOfBits() {
        var mapping = TgSingleParameterMapping.ofBits("foo");

        var type = TgDataType.BITS;
        assertMapping(type, null, mapping);
        assertMapping(type, new boolean[] { true, false, true }, mapping);
    }

    @Test
    void testOfDate() {
        var mapping = TgSingleParameterMapping.ofDate("foo");

        var type = TgDataType.DATE;
        assertMapping(type, null, mapping);
        assertMapping(type, LocalDate.of(2023, 3, 16), mapping);
    }

    @Test
    void testOfTime() {
        var mapping = TgSingleParameterMapping.ofTime("foo");

        var type = TgDataType.TIME;
        assertMapping(type, null, mapping);
        assertMapping(type, LocalTime.of(23, 59, 59), mapping);
    }

    @Test
    void testOfDateTime() {
        var mapping = TgSingleParameterMapping.ofDateTime("foo");

        var type = TgDataType.DATE_TIME;
        assertMapping(type, null, mapping);
        assertMapping(type, LocalDateTime.of(2023, 3, 16, 23, 59, 59), mapping);
    }

    @Test
    void testOfOffsetTime() {
        var mapping = TgSingleParameterMapping.ofOffsetTime("foo");

        var type = TgDataType.OFFSET_TIME;
        assertMapping(type, null, mapping);
        assertMapping(type, OffsetTime.of(23, 59, 59, 0, ZoneOffset.ofHours(9)), mapping);
    }

    @Test
    void testOfOffsetDateTime() {
        var mapping = TgSingleParameterMapping.ofOffsetDateTime("foo");

        var type = TgDataType.OFFSET_DATE_TIME;
        assertMapping(type, null, mapping);
        assertMapping(type, OffsetDateTime.of(2023, 3, 16, 23, 59, 59, 0, ZoneOffset.ofHours(9)), mapping);
    }

    @Test
    void testOfZonedDateTime() {
        var mapping = TgSingleParameterMapping.ofZonedDateTime("foo");

        var type = TgDataType.ZONED_DATE_TIME;
        assertMapping(type, null, mapping);
        assertMapping(type, ZonedDateTime.of(2023, 3, 16, 23, 59, 59, 0, ZoneOffset.ofHours(9)), mapping);
    }

    @Test
    void testOfClass() {
        var mapping = TgSingleParameterMapping.of("foo", int.class);

        var type = TgDataType.INT;
        assertMapping(type, null, mapping);
        assertMapping(type, 123, mapping);

        assertThrows(IllegalArgumentException.class, () -> {
            TgSingleParameterMapping.of("foo", BigInteger.class);
        });
    }

    @Test
    void testOfDataType() {
        var mapping = TgSingleParameterMapping.of("foo", TgDataType.INT);

        var type = TgDataType.INT;
        assertMapping(type, null, mapping);
        assertMapping(type, 123, mapping);
    }

    private static <P> void assertMapping(TgDataType type, P parameter, TgSingleParameterMapping<P> actualMapping) {
        assertMapping(type, parameter, parameter, actualMapping);
    }

    private static <P> void assertMapping(TgDataType type, P value, P expectedValue, TgSingleParameterMapping<P> actualMapping) {
        {
            var list = actualMapping.toLowPlaceholderList();
            assertEquals(1, list.size());
            var actual = list.get(0);
            assertEquals("foo", actual.getName());
            assertEquals(type.getLowDataType(), actual.getAtomType());
        }
        {
            var list = actualMapping.toLowParameterList(value, null);
            assertEquals(1, list.size());
            var actual = list.get(0);
            assertEquals("foo", actual.getName());
            assertParameter(expectedValue, actual);
        }
        {
            assertEquals("TgSingleParameterMapping[:foo/*" + type + "*/]", actualMapping.toString());
        }
    }

    private static void assertParameter(Object expected, Parameter actual) {
        if (expected == null) {
            assertEquals(Parameters.ofNull("foo"), actual);
            return;
        }
        if (expected instanceof Boolean) {
            assertEquals(Parameters.of("foo", (Boolean) expected), actual);
            return;
        }
        if (expected instanceof Integer) {
            assertEquals(Parameters.of("foo", (Integer) expected), actual);
            return;
        }
        if (expected instanceof Long) {
            assertEquals(Parameters.of("foo", (Long) expected), actual);
            return;
        }
        if (expected instanceof Float) {
            assertEquals(Parameters.of("foo", (Float) expected), actual);
            return;
        }
        if (expected instanceof Double) {
            assertEquals(Parameters.of("foo", (Double) expected), actual);
            return;
        }
        if (expected instanceof BigDecimal) {
            assertEquals(Parameters.of("foo", (BigDecimal) expected), actual);
            return;
        }
        if (expected instanceof String) {
            assertEquals(Parameters.of("foo", (String) expected), actual);
            return;
        }
        if (expected instanceof byte[]) {
            assertEquals(Parameters.of("foo", (byte[]) expected), actual);
            return;
        }
        if (expected instanceof boolean[]) {
            assertEquals(Parameters.of("foo", (boolean[]) expected), actual);
            return;
        }
        if (expected instanceof LocalDate) {
            assertEquals(Parameters.of("foo", (LocalDate) expected), actual);
            return;
        }
        if (expected instanceof LocalTime) {
            assertEquals(Parameters.of("foo", (LocalTime) expected), actual);
            return;
        }
        if (expected instanceof LocalDateTime) {
            assertEquals(Parameters.of("foo", (LocalDateTime) expected), actual);
            return;
        }
        if (expected instanceof OffsetTime) {
            assertEquals(Parameters.of("foo", (OffsetTime) expected), actual);
            return;
        }
        if (expected instanceof OffsetDateTime) {
            assertEquals(Parameters.of("foo", (OffsetDateTime) expected), actual);
            return;
        }
        if (expected instanceof ZonedDateTime) {
            assertEquals(Parameters.of("foo", ((ZonedDateTime) expected).toOffsetDateTime()), actual);
            return;
        }
        throw new UnsupportedOperationException(expected.getClass().getName());
    }
}
