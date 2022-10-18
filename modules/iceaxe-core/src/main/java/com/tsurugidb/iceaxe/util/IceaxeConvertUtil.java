package com.tsurugidb.iceaxe.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.MessageFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * convert type utility
 */
public class IceaxeConvertUtil {

    public static final IceaxeConvertUtil INSTANCE = new IceaxeConvertUtil();

    /**
     * convert to Boolean
     *
     * @param obj value
     * @return value
     */
    @Nullable
    public Boolean toBoolean(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertBoolean(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(obj, e);
        }
        throw createException(obj, null);
    }

    @Nullable
    protected Boolean convertBoolean(@Nonnull Object obj) {
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
        return null;
    }

    /**
     * convert to Integer
     *
     * @param obj value
     * @return value
     */
    @Nullable
    public Integer toInt4(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertInteger(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(obj, e);
        }
        throw createException(obj, null);
    }

    @Nullable
    protected Integer convertInteger(@Nonnull Object obj) {
        if (obj instanceof Number) {
            return ((Number) obj).intValue();
        }
        if (obj instanceof String) {
            return Integer.parseInt((String) obj);
        }
        return null;
    }

    /**
     * convert to Long
     *
     * @param obj value
     * @return value
     */
    @Nullable
    public Long toInt8(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertLong(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(obj, e);
        }
        throw createException(obj, null);
    }

    @Nullable
    protected Long convertLong(@Nonnull Object obj) {
        if (obj instanceof Number) {
            return ((Number) obj).longValue();
        }
        if (obj instanceof String) {
            return Long.parseLong((String) obj);
        }
        return null;
    }

    /**
     * convert to Float
     *
     * @param obj value
     * @return value
     */
    @Nullable
    public Float toFloat4(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertFloat(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(obj, e);
        }
        throw createException(obj, null);
    }

    @Nullable
    protected Float convertFloat(@Nonnull Object obj) {
        if (obj instanceof Number) {
            return ((Number) obj).floatValue();
        }
        if (obj instanceof String) {
            return Float.parseFloat((String) obj);
        }
        return null;
    }

    /**
     * convert to Double
     *
     * @param obj value
     * @return value
     */
    @Nullable
    public Double toFloat8(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertDouble(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(obj, e);
        }
        throw createException(obj, null);
    }

    @Nullable
    protected Double convertDouble(@Nonnull Object obj) {
        if (obj instanceof Number) {
            return ((Number) obj).doubleValue();
        }
        if (obj instanceof String) {
            return Double.parseDouble((String) obj);
        }
        return null;
    }

    /**
     * convert to BigDecimal
     *
     * @param obj value
     * @return value
     */
    @Nullable
    public BigDecimal toDecimal(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertBigDecimal(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(obj, e);
        }
        throw createException(obj, null);
    }

    @Nullable
    protected BigDecimal convertBigDecimal(@Nonnull Object obj) {
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
        return null;
    }

    /**
     * convert to String
     *
     * @param obj value
     * @return value
     */
    @Nullable
    public String toCharacter(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertString(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(obj, e);
        }
        throw createException(obj, null);
    }

    @Nullable
    protected String convertString(@Nonnull Object obj) {
        if (obj instanceof String) {
            return (String) obj;
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
    public byte[] toBytes(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertBytes(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(obj, e);
        }
        throw createException(obj, null);
    }

    @Nullable
    protected byte[] convertBytes(@Nonnull Object obj) {
        if (obj instanceof byte[]) {
            return (byte[]) obj;
        }
        return null;
    }

    /**
     * convert to boolean[]
     *
     * @param obj value
     * @return value
     */
    @Nullable
    public boolean[] toBits(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertBits(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(obj, e);
        }
        throw createException(obj, null);
    }

    @Nullable
    protected boolean[] convertBits(@Nonnull Object obj) {
        if (obj instanceof boolean[]) {
            return (boolean[]) obj;
        }
        return null;
    }

    /**
     * convert to date
     *
     * @param obj value
     * @return value
     */
    @Nullable
    public LocalDate toDate(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertLocalDate(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(obj, e);
        }
        throw createException(obj, null);
    }

    @Nullable
    protected LocalDate convertLocalDate(@Nonnull Object obj) {
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
        if (obj instanceof CharSequence) {
            return LocalDate.parse((CharSequence) obj);
        }
        return null;
    }

    /**
     * convert to time
     *
     * @param obj value
     * @return value
     */
    @Nullable
    public LocalTime toTime(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertLocalTime(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(obj, e);
        }
        throw createException(obj, null);
    }

    @Nullable
    protected LocalTime convertLocalTime(@Nonnull Object obj) {
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
        if (obj instanceof CharSequence) {
            return LocalTime.parse((CharSequence) obj);
        }
        return null;
    }

    /**
     * convert to dateTime
     *
     * @param obj value
     * @return value
     */
    @Nullable
    public LocalDateTime toDateTime(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertDateTime(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(obj, e);
        }
        throw createException(obj, null);
    }

    @Nullable
    protected LocalDateTime convertDateTime(@Nonnull Object obj) {
        if (obj instanceof LocalDateTime) {
            return ((LocalDateTime) obj);
        }
        // FIXME ZonedDateTime, OffsetDateTimeからLocalDateTimeへ変換してもよいのか？
        if (obj instanceof ZonedDateTime) {
            return ((ZonedDateTime) obj).toLocalDateTime();
        }
        if (obj instanceof OffsetDateTime) {
            return ((OffsetDateTime) obj).toLocalDateTime();
        }
        if (obj instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) obj).toLocalDateTime();
        }
        if (obj instanceof CharSequence) {
            return LocalDateTime.parse((CharSequence) obj);
        }
        return null;
    }

    /**
     * convert to offset time
     *
     * @param obj value
     * @return value
     */
    @Nullable
    public OffsetTime toOffsetTime(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertOffsetTime(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(obj, e);
        }
        throw createException(obj, null);
    }

    @Nullable
    protected OffsetTime convertOffsetTime(@Nonnull Object obj) {
        if (obj instanceof OffsetTime) {
            return ((OffsetTime) obj);
        }
        if (obj instanceof ZonedDateTime) {
            return ((ZonedDateTime) obj).toOffsetDateTime().toOffsetTime();
        }
        if (obj instanceof OffsetDateTime) {
            return ((OffsetDateTime) obj).toOffsetTime();
        }
        if (obj instanceof CharSequence) {
            return OffsetTime.parse((CharSequence) obj);
        }
        return null;
    }

    /**
     * convert to offset dateTime
     *
     * @param obj value
     * @return value
     */
    @Nullable
    public OffsetDateTime toOffsetDateTime(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertOffsetDateTime(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(obj, e);
        }
        throw createException(obj, null);
    }

    @Nullable
    protected OffsetDateTime convertOffsetDateTime(@Nonnull Object obj) {
        if (obj instanceof OffsetDateTime) {
            return ((OffsetDateTime) obj);
        }
        if (obj instanceof ZonedDateTime) {
            return ((ZonedDateTime) obj).toOffsetDateTime();
        }
        if (obj instanceof CharSequence) {
            return OffsetDateTime.parse((CharSequence) obj);
        }
        return null;
    }

    /**
     * convert to ZonedDateTime
     *
     * @param obj  value
     * @param zone time-zone
     * @return value
     */
    @Nullable
    public ZonedDateTime toZonedDateTime(@Nullable Object obj, @Nonnull ZoneId zone) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertZonedDateTime(obj, zone);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(obj, e);
        }
        throw createException(obj, null);
    }

    @Nullable
    protected ZonedDateTime convertZonedDateTime(@Nonnull Object obj, @Nonnull ZoneId zone) {
        if (obj instanceof ZonedDateTime) {
            return ((ZonedDateTime) obj).withZoneSameInstant(zone);
        }
        if (obj instanceof OffsetDateTime) {
            return ((OffsetDateTime) obj).atZoneSameInstant(zone);
        }
        if (obj instanceof LocalDateTime) {
            return ((LocalDateTime) obj).atZone(zone);
        }
        var instant = convertInstant(obj);
        if (instant != null) {
            return ZonedDateTime.ofInstant(instant, zone);
        }
        return null;
    }

    @Nullable
    protected Instant convertInstant(@Nonnull Object obj) {
        if (obj instanceof Instant) {
            return (Instant) obj;
        }
        if (obj instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) obj).toInstant();
        }
        return null;
    }

    protected RuntimeException createException(Object obj, Throwable cause) {
        return new UnsupportedOperationException(MessageFormat.format("unsupported type error. value={0}({1})", obj, obj.getClass()), cause);
    }
}
