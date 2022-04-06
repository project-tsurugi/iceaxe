package com.tsurugi.iceaxe.util;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class IceaxeConvertUtilTest {

    @Test
    void testToInt4() {
        assertNull(IceaxeConvertUtil.toInt4(null));
        assertEquals(Integer.valueOf(123), IceaxeConvertUtil.toInt4(123));
        assertEquals(Integer.valueOf(123), IceaxeConvertUtil.toInt4(123L));
        assertEquals(Integer.valueOf(123), IceaxeConvertUtil.toInt4(123f));
        assertEquals(Integer.valueOf(123), IceaxeConvertUtil.toInt4(123d));
        assertEquals(Integer.valueOf(123), IceaxeConvertUtil.toInt4("123"));
    }

    @Test
    void testToInt8() {
        assertNull(IceaxeConvertUtil.toInt8(null));
        assertEquals(Long.valueOf(123), IceaxeConvertUtil.toInt8(123));
        assertEquals(Long.valueOf(123), IceaxeConvertUtil.toInt8(123L));
        assertEquals(Long.valueOf(123), IceaxeConvertUtil.toInt8(123f));
        assertEquals(Long.valueOf(123), IceaxeConvertUtil.toInt8(123d));
        assertEquals(Long.valueOf(123), IceaxeConvertUtil.toInt8("123"));
    }

    @Test
    void testToFloat4() {
        assertNull(IceaxeConvertUtil.toFloat4(null));
        assertEquals(Float.valueOf(123), IceaxeConvertUtil.toFloat4(123));
        assertEquals(Float.valueOf(123), IceaxeConvertUtil.toFloat4(123L));
        assertEquals(Float.valueOf(123), IceaxeConvertUtil.toFloat4(123f));
        assertEquals(Float.valueOf(123), IceaxeConvertUtil.toFloat4(123d));
        assertEquals(Float.valueOf(123), IceaxeConvertUtil.toFloat4("123"));
    }

    @Test
    void testToFloat8() {
        assertNull(IceaxeConvertUtil.toFloat8(null));
        assertEquals(Double.valueOf(123), IceaxeConvertUtil.toFloat8(123));
        assertEquals(Double.valueOf(123), IceaxeConvertUtil.toFloat8(123L));
        assertEquals(Double.valueOf(123), IceaxeConvertUtil.toFloat8(123f));
        assertEquals(Double.valueOf(123), IceaxeConvertUtil.toFloat8(123d));
        assertEquals(Double.valueOf(123), IceaxeConvertUtil.toFloat8("123"));
    }

    @Test
    void testToCharacter() {
        assertNull(IceaxeConvertUtil.toCharacter(null));
        assertEquals("123", IceaxeConvertUtil.toCharacter(123));
        assertEquals("123", IceaxeConvertUtil.toCharacter(123L));
        assertEquals("123.0", IceaxeConvertUtil.toCharacter(123f));
        assertEquals("123.0", IceaxeConvertUtil.toCharacter(123d));
        assertEquals("123", IceaxeConvertUtil.toCharacter("123"));
    }
}
