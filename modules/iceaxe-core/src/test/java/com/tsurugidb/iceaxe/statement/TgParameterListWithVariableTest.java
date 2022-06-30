package com.tsurugidb.iceaxe.statement;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

class TgParameterListWithVariableTest {

    @Test
    void testGetType() {
        {
            var variable = TgVariableList.of();
            var list = TgParameterList.of(variable);
            var e = assertThrows(IllegalArgumentException.class, () -> list.bool("foo", true));
            assertEquals("not found type. name=foo", e.getMessage());
        }

        var variable = TgVariableList.of().int4("foo");
        var list = TgParameterList.of(variable);
        assertEquals(TgDataType.INT4, list.getType("foo"));
    }

    @Test
    void testBoolStringBoolean() {
        {
            var variable = TgVariableList.of().instant("foo");
            var list = TgParameterList.of(variable);
            assertThrows(IllegalStateException.class, () -> list.bool("foo", true));
        }

        var variable = TgVariableList.of().bool("foo");
        var list = TgParameterList.of(variable);
        list.bool("foo", true);
        assertParameterList(TgParameter.of("foo", true), list);
    }

    @Test
    void testBoolStringBooleanWrapper() {
        {
            var variable = TgVariableList.of().instant("foo");
            var list = TgParameterList.of(variable);
            assertThrows(IllegalStateException.class, () -> list.bool("foo", null));
        }

        var variable = TgVariableList.of().bool("foo").bool("bar");
        var list = TgParameterList.of(variable);
        list.bool("foo", Boolean.TRUE);
        list.bool("bar", null);
        assertParameterList(TgParameter.of("foo", Boolean.TRUE), TgParameter.of("bar", (Boolean) null), list);
    }

    @Test
    void testInt4StringInt() {
        {
            var variable = TgVariableList.of().instant("foo");
            var list = TgParameterList.of(variable);
            assertThrows(IllegalStateException.class, () -> list.int4("foo", 123));
        }

        var variable = TgVariableList.of().int4("foo");
        var list = TgParameterList.of(variable);
        list.int4("foo", 123);
        assertParameterList(TgParameter.of("foo", 123), list);
    }

    @Test
    void testInt4StringInteger() {
        {
            var variable = TgVariableList.of().instant("foo");
            var list = TgParameterList.of(variable);
            assertThrows(IllegalStateException.class, () -> list.int4("foo", null));
        }

        var variable = TgVariableList.of().int4("foo").int4("bar");
        var list = TgParameterList.of(variable);
        list.int4("foo", Integer.valueOf(123));
        list.int4("bar", null);
        assertParameterList(TgParameter.of("foo", 123), TgParameter.of("bar", (Integer) null), list);
    }

    @Test
    void testInt8StringLong() {
        {
            var variable = TgVariableList.of().instant("foo");
            var list = TgParameterList.of(variable);
            assertThrows(IllegalStateException.class, () -> list.int8("foo", 123));
        }

        var variable = TgVariableList.of().int8("foo");
        var list = TgParameterList.of(variable);
        list.int8("foo", 123);
        assertParameterList(TgParameter.of("foo", 123L), list);
    }

    @Test
    void testInt8StringLongWrapper() {
        {
            var variable = TgVariableList.of().instant("foo");
            var list = TgParameterList.of(variable);
            assertThrows(IllegalStateException.class, () -> list.int8("foo", null));
        }

        var variable = TgVariableList.of().int8("foo").int8("bar");
        var list = TgParameterList.of(variable);
        list.int8("foo", Long.valueOf(123));
        list.int8("bar", null);
        assertParameterList(TgParameter.of("foo", 123L), TgParameter.of("bar", (Long) null), list);
    }

    @Test
    void testFloat4StringFloat() {
        {
            var variable = TgVariableList.of().instant("foo");
            var list = TgParameterList.of(variable);
            assertThrows(IllegalStateException.class, () -> list.float4("foo", 123));
        }

        var variable = TgVariableList.of().float4("foo");
        var list = TgParameterList.of(variable);
        list.float4("foo", 123);
        assertParameterList(TgParameter.of("foo", 123f), list);
    }

    @Test
    void testFloat4StringFloatWrapper() {
        {
            var variable = TgVariableList.of().instant("foo");
            var list = TgParameterList.of(variable);
            assertThrows(IllegalStateException.class, () -> list.float4("foo", null));
        }

        var variable = TgVariableList.of().float4("foo").float4("bar");
        var list = TgParameterList.of(variable);
        list.float4("foo", Float.valueOf(123));
        list.float4("bar", null);
        assertParameterList(TgParameter.of("foo", 123f), TgParameter.of("bar", (Float) null), list);
    }

    @Test
    void testFloat8StringDouble() {
        {
            var variable = TgVariableList.of().instant("foo");
            var list = TgParameterList.of(variable);
            assertThrows(IllegalStateException.class, () -> list.float8("foo", 123));
        }

        var variable = TgVariableList.of().float8("foo");
        var list = TgParameterList.of(variable);
        list.float8("foo", 123);
        assertParameterList(TgParameter.of("foo", 123d), list);
    }

    @Test
    void testFloat8StringDoubleWrapper() {
        {
            var variable = TgVariableList.of().instant("foo");
            var list = TgParameterList.of(variable);
            assertThrows(IllegalStateException.class, () -> list.float8("foo", null));
        }

        var variable = TgVariableList.of().float8("foo").float8("bar");
        var list = TgParameterList.of(variable);
        list.float8("foo", Double.valueOf(123));
        list.float8("bar", null);
        assertParameterList(TgParameter.of("foo", 123d), TgParameter.of("bar", (Double) null), list);
    }

    @Test
    void testDecimal() {
        {
            var variable = TgVariableList.of().instant("foo");
            var list = TgParameterList.of(variable);
            assertThrows(IllegalStateException.class, () -> list.decimal("foo", null));
        }

        var variable = TgVariableList.of().decimal("foo").decimal("bar");
        var list = TgParameterList.of(variable);
        list.decimal("foo", BigDecimal.valueOf(123));
        list.decimal("bar", null);
        assertParameterList(TgParameter.of("foo", BigDecimal.valueOf(123)), TgParameter.of("bar", (BigDecimal) null), list);
    }

    @Test
    void testCharacter() {
        {
            var variable = TgVariableList.of().instant("foo");
            var list = TgParameterList.of(variable);
            assertThrows(IllegalStateException.class, () -> list.character("foo", "abc"));
        }

        var variable = TgVariableList.of().character("foo").character("bar");
        var list = TgParameterList.of(variable);
        list.character("foo", "abc");
        list.character("bar", null);
        assertParameterList(TgParameter.of("foo", "abc"), TgParameter.of("bar", (String) null), list);
    }

    @Test
    void testBytes() {
        {
            var variable = TgVariableList.of().instant("foo");
            var list = TgParameterList.of(variable);
            assertThrows(IllegalStateException.class, () -> list.bytes("foo", null));
        }

        var variable = TgVariableList.of().bytes("foo").bytes("bar");
        var list = TgParameterList.of(variable);
        list.bytes("foo", new byte[] { 1, 2, 3 });
        list.bytes("bar", null);
        assertParameterList(TgParameter.of("foo", new byte[] { 1, 2, 3 }), TgParameter.of("bar", (byte[]) null), list);
    }

    @Test
    void testBits() {
        {
            var variable = TgVariableList.of().instant("foo");
            var list = TgParameterList.of(variable);
            assertThrows(IllegalStateException.class, () -> list.bits("foo", null));
        }

        var variable = TgVariableList.of().bits("foo").bits("bar");
        var list = TgParameterList.of(variable);
        list.bits("foo", new boolean[] { true, false, true });
        list.bits("bar", null);
        assertParameterList(TgParameter.of("foo", new boolean[] { true, false, true }), TgParameter.of("bar", (boolean[]) null), list);
    }

    @Test
    void testDate() {
        {
            var variable = TgVariableList.of().instant("foo");
            var list = TgParameterList.of(variable);
            assertThrows(IllegalStateException.class, () -> list.date("foo", null));
        }

        var variable = TgVariableList.of().date("foo").date("bar");
        var list = TgParameterList.of(variable);
        list.date("foo", LocalDate.of(2022, 6, 30));
        list.date("bar", null);
        assertParameterList(TgParameter.of("foo", LocalDate.of(2022, 6, 30)), TgParameter.of("bar", (LocalDate) null), list);
    }

    @Test
    void testTime() {
        {
            var variable = TgVariableList.of().instant("foo");
            var list = TgParameterList.of(variable);
            assertThrows(IllegalStateException.class, () -> list.time("foo", null));
        }

        var variable = TgVariableList.of().time("foo").time("bar");
        var list = TgParameterList.of(variable);
        list.time("foo", LocalTime.of(23, 30, 59));
        list.time("bar", null);
        assertParameterList(TgParameter.of("foo", LocalTime.of(23, 30, 59)), TgParameter.of("bar", (LocalTime) null), list);
    }

    @Test
    void testInstant() {
        {
            var variable = TgVariableList.of().int4("foo");
            var list = TgParameterList.of(variable);
            assertThrows(IllegalStateException.class, () -> list.instant("foo", null));
        }

        var instant = ZonedDateTime.of(2022, 6, 30, 23, 30, 59, 999, ZoneId.of("Asia/Tokyo")).toInstant();
        var variable = TgVariableList.of().instant("foo").instant("bar");
        var list = TgParameterList.of(variable);
        list.instant("foo", instant);
        list.instant("bar", null);
        assertParameterList(TgParameter.of("foo", instant), TgParameter.of("bar", (Instant) null), list);
    }

    @Test
    void testZonedDateTime() {
        {
            var variable = TgVariableList.of().int4("foo");
            var list = TgParameterList.of(variable);
            assertThrows(IllegalStateException.class, () -> list.zonedDateTime("foo", null));
        }

        var zdt = ZonedDateTime.of(2022, 6, 30, 23, 30, 59, 999, ZoneId.of("Asia/Tokyo"));
        var variable = TgVariableList.of().zonedDateTime("foo").zonedDateTime("bar");
        var list = TgParameterList.of(variable);
        list.zonedDateTime("foo", zdt);
        list.zonedDateTime("bar", null);
        assertParameterList(TgParameter.of("foo", zdt), TgParameter.of("bar", (ZonedDateTime) null), list);
    }

    @Test
    void testAddStringObject() {
        {
            var list = TgParameterList.of(TgVariableList.of().bool("foo"));
            list.add("foo", 123);
            assertParameterList(TgParameter.of("foo", true), list);
        }
        {
            var list = TgParameterList.of(TgVariableList.of().int4("foo"));
            list.add("foo", "123");
            assertParameterList(TgParameter.of("foo", 123), list);
        }
        {
            var list = TgParameterList.of(TgVariableList.of().int8("foo"));
            list.add("foo", 123);
            assertParameterList(TgParameter.of("foo", 123L), list);
        }
        {
            var list = TgParameterList.of(TgVariableList.of().float4("foo"));
            list.add("foo", 123);
            assertParameterList(TgParameter.of("foo", 123f), list);
        }
        {
            var list = TgParameterList.of(TgVariableList.of().float8("foo"));
            list.add("foo", 123);
            assertParameterList(TgParameter.of("foo", 123d), list);
        }
        {
            var list = TgParameterList.of(TgVariableList.of().decimal("foo"));
            list.add("foo", 123);
            assertParameterList(TgParameter.of("foo", BigDecimal.valueOf(123)), list);
        }
        {
            var list = TgParameterList.of(TgVariableList.of().character("foo"));
            list.add("foo", 123);
            assertParameterList(TgParameter.of("foo", "123"), list);
        }
        {
            var list = TgParameterList.of(TgVariableList.of().bytes("foo"));
            list.add("foo", new byte[] { 1, 2, 3 });
            assertParameterList(TgParameter.of("foo", new byte[] { 1, 2, 3 }), list);
        }
        {
            var list = TgParameterList.of(TgVariableList.of().bits("foo"));
            list.add("foo", new boolean[] { true, false, true });
            assertParameterList(TgParameter.of("foo", new boolean[] { true, false, true }), list);
        }
        {
            var list = TgParameterList.of(TgVariableList.of().date("foo"));
            list.add("foo", LocalDateTime.of(2022, 6, 30, 23, 30, 59));
            assertParameterList(TgParameter.of("foo", LocalDate.of(2022, 6, 30)), list);
        }
        {
            var list = TgParameterList.of(TgVariableList.of().time("foo"));
            list.add("foo", LocalDateTime.of(2022, 6, 30, 23, 30, 59));
            assertParameterList(TgParameter.of("foo", LocalTime.of(23, 30, 59)), list);
        }
        {
            var zdt = ZonedDateTime.of(2022, 6, 30, 23, 30, 59, 999, ZoneId.of("Asia/Tokyo"));
            var list = TgParameterList.of(TgVariableList.of().instant("foo"));
            list.add("foo", zdt);
            assertParameterList(TgParameter.of("foo", zdt.toInstant()), list);
        }
    }

    @Test
    void testAddTgParameterList() {
        var variable = TgVariableList.of().int4("foo").character("bar");
        var list = TgParameterList.of(variable);
        list.add("foo", 123);
        list.add("bar", "abc");

        var variable2 = TgVariableList.of().int4("zzz1").int4("zzz2");
        var list2 = TgParameterList.of(variable2);
        list2.add("zzz1", 123);
        list2.add("zzz2", 456);

        list.add(list2);

        assertParameterList(List.of(TgParameter.of("foo", 123), TgParameter.of("bar", "abc"), TgParameter.of("zzz1", 123), TgParameter.of("zzz2", 456)), list);
        assertParameterList(TgParameter.of("zzz1", 123), TgParameter.of("zzz2", 456), list2);
    }

    private void assertParameterList(TgParameter expected, TgParameterListWithVariable actual) {
        assertParameterList(List.of(expected), actual);
    }

    private void assertParameterList(TgParameter expected1, TgParameter expected2, TgParameterListWithVariable actual) {
        assertParameterList(List.of(expected1, expected2), actual);
    }

    private void assertParameterList(List<TgParameter> expected, TgParameterListWithVariable actual) {
        var expectedLow = expected.stream().map(TgParameter::toLowParameter).collect(Collectors.toList());
        var actualLow = actual.toLowParameterList();
        assertEquals(expectedLow, actualLow);
    }

    @Test
    void testToString() {
        var variable = TgVariableList.of().int4("foo").character("bar");
        var empty = TgParameterList.of(variable);
        assertEquals("TgParameterListWithVariable[]", empty.toString());
    }
}
