package com.tsurugidb.iceaxe.util;

// internal
public class IceaxeConvertUtil {

    public static Integer toInt4(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Number) {
            return ((Number) obj).intValue();
        }
        if (obj instanceof String) {
            return Integer.parseInt((String) obj);
        }
        throw createException(obj);
    }

    public static Long toInt8(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Number) {
            return ((Number) obj).longValue();
        }
        if (obj instanceof String) {
            return Long.parseLong((String) obj);
        }
        throw createException(obj);
    }

    public static Float toFloat4(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Number) {
            return ((Number) obj).floatValue();
        }
        if (obj instanceof String) {
            return Float.parseFloat((String) obj);
        }
        throw createException(obj);
    }

    public static Double toFloat8(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Number) {
            return ((Number) obj).doubleValue();
        }
        if (obj instanceof String) {
            return Double.parseDouble((String) obj);
        }
        throw createException(obj);
    }

    public static String toCharacter(Object obj) {
        if (obj == null) {
            return null;
        }
        return obj.toString();
    }

    private static UnsupportedOperationException createException(Object obj) {
        return new UnsupportedOperationException("unsupported type error. value=" + obj + ", class=" + obj.getClass());
    }
}
