package com.tsurugidb.iceaxe.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * convert type utility
 */
public final class IceaxeConvertUtil {

    private IceaxeConvertUtil() {
        // do nothing
    }

    /**
     * convert to Boolean
     * 
     * @param obj value
     * @return value
     */
    @Nullable
    public static Boolean toBoolean(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Boolean) {
            return (Boolean) obj;
        }
        if (obj instanceof String) {
            return Boolean.valueOf((String) obj);
        }
        if (obj instanceof Byte || obj instanceof Short || obj instanceof Integer || obj instanceof Long) {
            return ((Number) obj).longValue() != 0;
        }
        if (obj instanceof Float || obj instanceof Double) {
            return ((Number) obj).doubleValue() != 0;
        }
        if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).compareTo(BigDecimal.ZERO) != 0;
        }
        if (obj instanceof BigInteger) {
            return ((BigInteger) obj).compareTo(BigInteger.ZERO) != 0;
        }
        throw createException(obj);
    }

    /**
     * convert to Integer
     * 
     * @param obj value
     * @return value
     */
    @Nullable
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

    /**
     * convert to Long
     * 
     * @param obj value
     * @return value
     */
    @Nullable
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

    /**
     * convert to Float
     * 
     * @param obj value
     * @return value
     */
    @Nullable
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

    /**
     * convert to Double
     * 
     * @param obj value
     * @return value
     */
    @Nullable
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

    /**
     * convert to BigDecimal
     * 
     * @param obj value
     * @return value
     */
    @Nullable
    public static BigDecimal toDecimal(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof BigDecimal) {
            return (BigDecimal) obj;
        }
        if (obj instanceof BigInteger) {
            return new BigDecimal((BigInteger) obj);
        }
        if (obj instanceof Byte || obj instanceof Short || obj instanceof Integer || obj instanceof Long) {
            return BigDecimal.valueOf(((Number) obj).longValue());
        }
        if (obj instanceof Float || obj instanceof Double) {
            return BigDecimal.valueOf(((Number) obj).doubleValue());
        }
        if (obj instanceof Number) {
            return new BigDecimal(obj.toString());
        }
        if (obj instanceof String) {
            return new BigDecimal((String) obj);
        }
        throw createException(obj);
    }

    /**
     * convert to String
     * 
     * @param obj value
     * @return value
     */
    @Nullable
    public static String toCharacter(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).toPlainString();
        }
        return obj.toString();
    }

    /**
     * convert to byte[]
     * 
     * @param obj value
     * @return value
     */
    @Nullable
    public static byte[] toBytes(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof byte[]) {
            return (byte[]) obj;
        }
        throw createException(obj);
    }

    /**
     * convert to boolean[]
     * 
     * @param obj value
     * @return value
     */
    @Nullable
    public static boolean[] toBits(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof boolean[]) {
            return (boolean[]) obj;
        }
        throw createException(obj);
    }

    /**
     * convert to date
     * 
     * @param obj value
     * @return value
     */
    @Nullable
    public static LocalDate toDate(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof LocalDate) {
            return (LocalDate) obj;
        }
        if (obj instanceof LocalDateTime) {
            return ((LocalDateTime) obj).toLocalDate();
        }
        // FIXME ZonedDateTime, OffsetDateTimeからLocalDateへ変換してもよいのか？
        if (obj instanceof ZonedDateTime) {
            return ((ZonedDateTime) obj).toLocalDate();
        }
        if (obj instanceof OffsetDateTime) {
            return ((OffsetDateTime) obj).toLocalDate();
        }
        if (obj instanceof java.sql.Date) {
            return ((java.sql.Date) obj).toLocalDate();
        }
        if (obj instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) obj).toLocalDateTime().toLocalDate();
        }
        throw createException(obj);
    }

    /**
     * convert to time
     * 
     * @param obj value
     * @return value
     */
    @Nullable
    public static LocalTime toTime(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof LocalTime) {
            return (LocalTime) obj;
        }
        if (obj instanceof LocalDateTime) {
            return ((LocalDateTime) obj).toLocalTime();
        }
        // FIXME ZonedDateTime, OffsetDateTimeからLocalTimeへ変換してもよいのか？
        if (obj instanceof ZonedDateTime) {
            return ((ZonedDateTime) obj).toLocalTime();
        }
        if (obj instanceof OffsetDateTime) {
            return ((OffsetDateTime) obj).toLocalTime();
        }
        if (obj instanceof java.sql.Time) {
            return ((java.sql.Time) obj).toLocalTime();
        }
        if (obj instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) obj).toLocalDateTime().toLocalTime();
        }
        throw createException(obj);
    }

    /**
     * convert to Instant
     * 
     * @param obj value
     * @return value
     */
    @Nullable
    public static Instant toInstant(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Instant) {
            return (Instant) obj;
        }
        if (obj instanceof ZonedDateTime) {
            return ((ZonedDateTime) obj).toInstant();
        }
        if (obj instanceof OffsetDateTime) {
            return ((OffsetDateTime) obj).toInstant();
        }
        if (obj instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) obj).toInstant();
        }
        throw createException(obj);
    }

    /**
     * convert to ZonedDateTime
     * 
     * @param obj  value
     * @param zone time-zone
     * @return value
     */
    @Nullable
    public static ZonedDateTime toZonedDateTime(Object obj, @Nonnull ZoneId zone) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof ZonedDateTime) {
            return ((ZonedDateTime) obj).withZoneSameInstant(zone);
        }
        if (obj instanceof OffsetDateTime) {
            return ((OffsetDateTime) obj).atZoneSameInstant(zone);
        }
        if (obj instanceof LocalDateTime) {
            return ((LocalDateTime) obj).atZone(zone);
        }
        var instant = toInstant(obj);
        return ZonedDateTime.ofInstant(instant, zone);
    }

    private static UnsupportedOperationException createException(Object obj) {
        return new UnsupportedOperationException("unsupported type error. value=" + obj + ", class=" + obj.getClass());
    }
}
