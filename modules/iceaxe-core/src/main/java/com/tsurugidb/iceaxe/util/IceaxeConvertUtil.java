/*
 * Copyright 2023-2024 Project Tsurugi.
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
 * convert type utility.
 */
public class IceaxeConvertUtil {

    /** convert type utility singleton instance */
    public static final IceaxeConvertUtil INSTANCE = new IceaxeConvertUtil();

    /**
     * convert to Boolean.
     *
     * @param obj value
     * @return value
     */
    public @Nullable Boolean toBoolean(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertBoolean(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(boolean.class, obj, e);
        }
        throw createException(boolean.class, obj, null);
    }

    /**
     * convert to Boolean.
     *
     * @param obj value
     * @return value
     */
    protected @Nullable Boolean convertBoolean(@Nonnull Object obj) {
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
     * convert to Integer.
     *
     * @param obj value
     * @return value
     */
    public @Nullable Integer toInt(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertInteger(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(int.class, obj, e);
        }
        throw createException(int.class, obj, null);
    }

    /**
     * convert to Integer.
     *
     * @param obj value
     * @return value
     */
    protected @Nullable Integer convertInteger(@Nonnull Object obj) {
        if (obj instanceof Integer) {
            return (Integer) obj;
        }
        if (obj instanceof Number) {
            return ((Number) obj).intValue();
        }
        if (obj instanceof String) {
            return Integer.parseInt((String) obj);
        }
        return null;
    }

    /**
     * convert to Long.
     *
     * @param obj value
     * @return value
     */
    public @Nullable Long toLong(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertLong(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(long.class, obj, e);
        }
        throw createException(long.class, obj, null);
    }

    /**
     * convert to Long.
     *
     * @param obj value
     * @return value
     */
    protected @Nullable Long convertLong(@Nonnull Object obj) {
        if (obj instanceof Long) {
            return (Long) obj;
        }
        if (obj instanceof Number) {
            return ((Number) obj).longValue();
        }
        if (obj instanceof String) {
            return Long.parseLong((String) obj);
        }
        return null;
    }

    /**
     * convert to Float.
     *
     * @param obj value
     * @return value
     */
    public @Nullable Float toFloat(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertFloat(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(float.class, obj, e);
        }
        throw createException(float.class, obj, null);
    }

    /**
     * convert to Float.
     *
     * @param obj value
     * @return value
     */
    protected @Nullable Float convertFloat(@Nonnull Object obj) {
        if (obj instanceof Float) {
            return (Float) obj;
        }
        if (obj instanceof Number) {
            return ((Number) obj).floatValue();
        }
        if (obj instanceof String) {
            return Float.parseFloat((String) obj);
        }
        return null;
    }

    /**
     * convert to Double.
     *
     * @param obj value
     * @return value
     */
    public @Nullable Double toDouble(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertDouble(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(double.class, obj, e);
        }
        throw createException(double.class, obj, null);
    }

    /**
     * convert to Double.
     *
     * @param obj value
     * @return value
     */
    protected @Nullable Double convertDouble(@Nonnull Object obj) {
        if (obj instanceof Double) {
            return (Double) obj;
        }
        if (obj instanceof Number) {
            return ((Number) obj).doubleValue();
        }
        if (obj instanceof String) {
            return Double.parseDouble((String) obj);
        }
        return null;
    }

    /**
     * convert to BigDecimal.
     *
     * @param obj value
     * @return value
     */
    public @Nullable BigDecimal toDecimal(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertBigDecimal(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(BigDecimal.class, obj, e);
        }
        throw createException(BigDecimal.class, obj, null);
    }

    /**
     * convert to BigDecimal.
     *
     * @param obj value
     * @return value
     */
    protected @Nullable BigDecimal convertBigDecimal(@Nonnull Object obj) {
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
     * convert to String.
     *
     * @param obj value
     * @return value
     */
    public @Nullable String toString(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertString(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(String.class, obj, e);
        }
        throw createException(String.class, obj, null);
    }

    /**
     * convert to String.
     *
     * @param obj value
     * @return value
     */
    protected @Nullable String convertString(@Nonnull Object obj) {
        if (obj instanceof String) {
            return (String) obj;
        }
        if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).toPlainString();
        }
        return obj.toString();
    }

    /**
     * convert to byte[].
     *
     * @param obj value
     * @return value
     */
    public @Nullable byte[] toBytes(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertBytes(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(byte[].class, obj, e);
        }
        throw createException(byte[].class, obj, null);
    }

    /**
     * convert to byte[].
     *
     * @param obj value
     * @return value
     */
    protected @Nullable byte[] convertBytes(@Nonnull Object obj) {
        if (obj instanceof byte[]) {
            return (byte[]) obj;
        }
        return null;
    }

    /**
     * convert to boolean[].
     *
     * @param obj value
     * @return value
     */
    public @Nullable boolean[] toBits(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertBits(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(boolean[].class, obj, e);
        }
        throw createException(boolean[].class, obj, null);
    }

    /**
     * convert to boolean[].
     *
     * @param obj value
     * @return value
     */
    protected @Nullable boolean[] convertBits(@Nonnull Object obj) {
        if (obj instanceof boolean[]) {
            return (boolean[]) obj;
        }
        return null;
    }

    /**
     * convert to date.
     *
     * @param obj value
     * @return value
     */
    public @Nullable LocalDate toDate(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertLocalDate(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(LocalDate.class, obj, e);
        }
        throw createException(LocalDate.class, obj, null);
    }

    /**
     * convert to date.
     *
     * @param obj value
     * @return value
     */
    protected @Nullable LocalDate convertLocalDate(@Nonnull Object obj) {
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
     * convert to time.
     *
     * @param obj value
     * @return value
     */
    public @Nullable LocalTime toTime(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertLocalTime(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(LocalTime.class, obj, e);
        }
        throw createException(LocalTime.class, obj, null);
    }

    /**
     * convert to time.
     *
     * @param obj value
     * @return value
     */
    protected @Nullable LocalTime convertLocalTime(@Nonnull Object obj) {
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
     * convert to dateTime.
     *
     * @param obj value
     * @return value
     */
    public @Nullable LocalDateTime toDateTime(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertDateTime(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(LocalDateTime.class, obj, e);
        }
        throw createException(LocalDateTime.class, obj, null);
    }

    /**
     * convert to dateTime.
     *
     * @param obj value
     * @return value
     */
    protected @Nullable LocalDateTime convertDateTime(@Nonnull Object obj) {
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
     * convert to offset time.
     *
     * @param obj value
     * @return value
     */
    public @Nullable OffsetTime toOffsetTime(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertOffsetTime(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(OffsetTime.class, obj, e);
        }
        throw createException(OffsetTime.class, obj, null);
    }

    /**
     * convert to offset time.
     *
     * @param obj value
     * @return value
     */
    protected @Nullable OffsetTime convertOffsetTime(@Nonnull Object obj) {
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
     * convert to offset dateTime.
     *
     * @param obj value
     * @return value
     */
    public @Nullable OffsetDateTime toOffsetDateTime(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertOffsetDateTime(obj);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(OffsetDateTime.class, obj, e);
        }
        throw createException(OffsetDateTime.class, obj, null);
    }

    /**
     * convert to offset dateTime.
     *
     * @param obj value
     * @return value
     */
    protected @Nullable OffsetDateTime convertOffsetDateTime(@Nonnull Object obj) {
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
     * convert to ZonedDateTime.
     *
     * @param obj  value
     * @param zone time-zone
     * @return value
     */
    public @Nullable ZonedDateTime toZonedDateTime(@Nullable Object obj, @Nonnull ZoneId zone) {
        if (obj == null) {
            return null;
        }
        try {
            var value = convertZonedDateTime(obj, zone);
            if (value != null) {
                return value;
            }
        } catch (Throwable e) {
            throw createException(ZonedDateTime.class, obj, e);
        }
        throw createException(ZonedDateTime.class, obj, null);
    }

    /**
     * convert to ZonedDateTime.
     *
     * @param obj  value
     * @param zone time-zone
     * @return value
     */
    protected @Nullable ZonedDateTime convertZonedDateTime(@Nonnull Object obj, @Nonnull ZoneId zone) {
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

    /**
     * convert to Instance.
     *
     * @param obj value
     * @return value
     */
    protected @Nullable Instant convertInstant(@Nonnull Object obj) {
        if (obj instanceof Instant) {
            return (Instant) obj;
        }
        if (obj instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) obj).toInstant();
        }
        return null;
    }

    /**
     * Creates a new exception instance.
     *
     * @param toClass to class
     * @param obj     value
     * @param cause   cause exception
     * @return exception
     */
    protected RuntimeException createException(Class<?> toClass, @Nonnull Object obj, @Nullable Throwable cause) {
        return new UnsupportedOperationException(MessageFormat.format("unsupported type. toClass={0}, value={1}({2})", toClass.getName(), obj, obj.getClass()), cause);
    }
}
