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
package com.tsurugidb.iceaxe.sql.result.mapping;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.sql.type.IceaxeObjectFactory;
import com.tsurugidb.iceaxe.sql.type.TgBlob;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.function.TsurugiTransactionFunction;

/**
 * Tsurugi Result Mapping for single column.
 *
 * @param <R> result type
 */
public class TgSingleResultMapping<R> extends TgResultMapping<R> {

    private static TgSingleResultMapping<Boolean> booleanMapping;

    /**
     * create result mapping.
     *
     * @return result mapping
     */
    public static TgSingleResultMapping<Boolean> ofBoolean() {
        if (booleanMapping == null) {
            booleanMapping = new TgSingleResultMapping<>(TsurugiResultRecord::nextBooleanOrNull);
        }
        return booleanMapping;
    }

    private static TgSingleResultMapping<Integer> intMapping;

    /**
     * create result mapping.
     *
     * @return result mapping
     */
    public static TgSingleResultMapping<Integer> ofInt() {
        if (intMapping == null) {
            intMapping = new TgSingleResultMapping<>(TsurugiResultRecord::nextIntOrNull);
        }
        return intMapping;
    }

    private static TgSingleResultMapping<Long> longMapping;

    /**
     * create result mapping.
     *
     * @return result mapping
     */
    public static TgSingleResultMapping<Long> ofLong() {
        if (longMapping == null) {
            longMapping = new TgSingleResultMapping<>(TsurugiResultRecord::nextLongOrNull);
        }
        return longMapping;
    }

    private static TgSingleResultMapping<Float> floatMapping;

    /**
     * create result mapping.
     *
     * @return result mapping
     */
    public static TgSingleResultMapping<Float> ofFloat() {
        if (floatMapping == null) {
            floatMapping = new TgSingleResultMapping<>(TsurugiResultRecord::nextFloatOrNull);
        }
        return floatMapping;
    }

    private static TgSingleResultMapping<Double> doubleMapping;

    /**
     * create result mapping.
     *
     * @return result mapping
     */
    public static TgSingleResultMapping<Double> ofDouble() {
        if (doubleMapping == null) {
            doubleMapping = new TgSingleResultMapping<>(TsurugiResultRecord::nextDoubleOrNull);
        }
        return doubleMapping;
    }

    private static TgSingleResultMapping<BigDecimal> decimalMapping;

    /**
     * create result mapping.
     *
     * @return result mapping
     */
    public static TgSingleResultMapping<BigDecimal> ofDecimal() {
        if (decimalMapping == null) {
            decimalMapping = new TgSingleResultMapping<>(TsurugiResultRecord::nextDecimalOrNull);
        }
        return decimalMapping;
    }

    private static TgSingleResultMapping<String> stringMapping;

    /**
     * create result mapping.
     *
     * @return result mapping
     */
    public static TgSingleResultMapping<String> ofString() {
        if (stringMapping == null) {
            stringMapping = new TgSingleResultMapping<>(TsurugiResultRecord::nextStringOrNull);
        }
        return stringMapping;
    }

    private static TgSingleResultMapping<byte[]> bytesMapping;

    /**
     * create result mapping.
     *
     * @return result mapping
     */
    public static TgSingleResultMapping<byte[]> ofBytes() {
        if (bytesMapping == null) {
            bytesMapping = new TgSingleResultMapping<>(TsurugiResultRecord::nextBytesOrNull);
        }
        return bytesMapping;
    }

    private static TgSingleResultMapping<boolean[]> bitsMapping;

    /**
     * <em>This method is not yet implemented:</em> create result mapping.
     *
     * @return result mapping
     */
    public static TgSingleResultMapping<boolean[]> ofBits() {
        if (bitsMapping == null) {
            bitsMapping = new TgSingleResultMapping<>(TsurugiResultRecord::nextBitsOrNull);
        }
        return bitsMapping;
    }

    private static TgSingleResultMapping<LocalDate> dateMapping;

    /**
     * create result mapping.
     *
     * @return result mapping
     */
    public static TgSingleResultMapping<LocalDate> ofDate() {
        if (dateMapping == null) {
            dateMapping = new TgSingleResultMapping<>(TsurugiResultRecord::nextDateOrNull);
        }
        return dateMapping;
    }

    private static TgSingleResultMapping<LocalTime> timeMapping;

    /**
     * create result mapping.
     *
     * @return result mapping
     */
    public static TgSingleResultMapping<LocalTime> ofTime() {
        if (timeMapping == null) {
            timeMapping = new TgSingleResultMapping<>(TsurugiResultRecord::nextTimeOrNull);
        }
        return timeMapping;
    }

    private static TgSingleResultMapping<LocalDateTime> dateTimeMapping;

    /**
     * create result mapping.
     *
     * @return result mapping
     */
    public static TgSingleResultMapping<LocalDateTime> ofDateTime() {
        if (dateTimeMapping == null) {
            dateTimeMapping = new TgSingleResultMapping<>(TsurugiResultRecord::nextDateTimeOrNull);
        }
        return dateTimeMapping;
    }

    private static TgSingleResultMapping<OffsetTime> offsetTimeMapping;

    /**
     * create result mapping.
     *
     * @return result mapping
     */
    public static TgSingleResultMapping<OffsetTime> ofOffsetTime() {
        if (offsetTimeMapping == null) {
            offsetTimeMapping = new TgSingleResultMapping<>(TsurugiResultRecord::nextOffsetTimeOrNull);
        }
        return offsetTimeMapping;
    }

    private static TgSingleResultMapping<OffsetDateTime> offsetDateTimeMapping;

    /**
     * create result mapping.
     *
     * @return result mapping
     */
    public static TgSingleResultMapping<OffsetDateTime> ofOffsetDateTime() {
        if (offsetDateTimeMapping == null) {
            offsetDateTimeMapping = new TgSingleResultMapping<>(TsurugiResultRecord::nextOffsetDateTimeOrNull);
        }
        return offsetDateTimeMapping;
    }

    private static TgSingleResultMapping<ZonedDateTime> zonedDateTimeMapping;

    /**
     * create result mapping.
     *
     * @param zone time-zone
     * @return result mapping
     */
    public static TgSingleResultMapping<ZonedDateTime> ofZonedDateTime(ZoneId zone) {
        if (zonedDateTimeMapping == null) {
            zonedDateTimeMapping = new TgSingleResultMapping<>(record -> record.nextZonedDateTimeOrNull(zone));
        }
        return zonedDateTimeMapping;
    }

    private static TgSingleResultMapping<TgBlob> blobMapping;

    /**
     * create result mapping.
     *
     * @return result mapping
     * @since X.X.X
     */
    public static TgSingleResultMapping<TgBlob> ofBlob() {
        if (blobMapping == null) {
            blobMapping = new TgSingleResultMapping<>(record -> {
                var value = record.nextBlobOrNull();
                var factory = IceaxeObjectFactory.getDefaultInstance();
                return factory.createBlob(value);
            });
        }
        return blobMapping;
    }

    private final TsurugiTransactionFunction<TsurugiResultRecord, R> resultConverter;

    /**
     * create result mapping.
     *
     * @param <R>   result type
     * @param clazz result type
     * @return result mapping
     */
    public static <R> TgSingleResultMapping<R> of(Class<R> clazz) {
        TgDataType type;
        try {
            type = TgDataType.of(clazz);
        } catch (UnsupportedOperationException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        return of(type);
    }

    /**
     * create result mapping.
     *
     * @param <R>  result type
     * @param type result type
     * @return result mapping
     */
    public static <R> TgSingleResultMapping<R> of(TgDataType type) {
        @SuppressWarnings("unchecked")
        var r = (TgSingleResultMapping<R>) ofSingleRaw(type);
        return r;
    }

    private static TgSingleResultMapping<?> ofSingleRaw(TgDataType type) {
        switch (type) {
        case BOOLEAN:
            return ofBoolean();
        case INT:
            return ofInt();
        case LONG:
            return ofLong();
        case FLOAT:
            return ofFloat();
        case DOUBLE:
            return ofDouble();
        case DECIMAL:
            return ofDecimal();
        case STRING:
            return ofString();
        case BYTES:
            return ofBytes();
        case BITS:
            return ofBits();
        case DATE:
            return ofDate();
        case TIME:
            return ofTime();
        case DATE_TIME:
            return ofDateTime();
        case OFFSET_TIME:
            return ofOffsetTime();
        case OFFSET_DATE_TIME:
            return ofOffsetDateTime();
        case BLOB:
            return ofBlob();
        // TODO CLOB
        case ZONED_DATE_TIME:
        default:
            throw new IllegalArgumentException(MessageFormat.format("unsupported type. type={0}", type));
        }
    }

    /**
     * Tsurugi Result Mapping for single column.
     *
     * @param resultConverter converter from TsurugiResultRecord to R
     */
    public TgSingleResultMapping(TsurugiTransactionFunction<TsurugiResultRecord, R> resultConverter) {
        this.resultConverter = resultConverter;
    }

    @Override
    protected R convert(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException {
        return resultConverter.apply(record);
    }
}
