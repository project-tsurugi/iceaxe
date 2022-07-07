package com.tsurugidb.iceaxe.result;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.tsurugidb.iceaxe.statement.TgDataType;
import com.tsurugidb.iceaxe.util.IoBiConsumer;
import com.tsurugidb.iceaxe.util.IoFunction;

/**
 * Tsurugi Result Mapping for Entity
 * 
 * @param <R> result type (e.g. Entity)
 */
public class TgEntityResultMapping<R> extends TgResultMapping<R> {

    /**
     * create Result Mapping
     * 
     * @param <R>            result type
     * @param entitySupplier supplier of R
     * @return Result Mapping
     */
    public static <R> TgEntityResultMapping<R> of(Supplier<R> entitySupplier) {
        return new TgEntityResultMapping<R>().supplier(entitySupplier);
    }

    private Supplier<R> entitySupplier;
    private final List<IoBiConsumer<R, TsurugiResultRecord>> columnConverterList = new ArrayList<>();
    private Map<String, IoBiConsumer<R, TsurugiResultRecord>> columnConverterMap;

    /**
     * Tsurugi Result Mapping
     */
    public TgEntityResultMapping() {
        // do nothing
    }

    /**
     * set supplier
     * 
     * @param entitySupplier supplier of R
     * @return this
     */
    public TgEntityResultMapping<R> supplier(Supplier<R> entitySupplier) {
        this.entitySupplier = entitySupplier;
        return this;
    }

    // boolean

    /**
     * add setter
     * 
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> bool(BiConsumer<R, Boolean> setter) {
        int index = columnConverterList.size();
        return bool(index, setter);
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> bool(BiConsumer<R, V> setter, Function<Boolean, V> converter) {
        int index = columnConverterList.size();
        return bool(index, setter, converter);
    }

    /**
     * add setter
     * 
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> bool(int index, BiConsumer<R, Boolean> setter) {
        set(index, TsurugiResultRecord::nextBooleanOrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> bool(int index, BiConsumer<R, V> setter, Function<Boolean, V> converter) {
        return bool(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter
     * 
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> bool(String name, BiConsumer<R, Boolean> setter) {
        set(name, TsurugiResultRecord::nextBooleanOrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> bool(String name, BiConsumer<R, V> setter, Function<Boolean, V> converter) {
        return bool(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // int4

    /**
     * add setter
     * 
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> int4(BiConsumer<R, Integer> setter) {
        int index = columnConverterList.size();
        return int4(index, setter);
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> int4(BiConsumer<R, V> setter, Function<Integer, V> converter) {
        int index = columnConverterList.size();
        return int4(index, setter, converter);
    }

    /**
     * add setter
     * 
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> int4(int index, BiConsumer<R, Integer> setter) {
        set(index, TsurugiResultRecord::nextInt4OrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> int4(int index, BiConsumer<R, V> setter, Function<Integer, V> converter) {
        return int4(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter
     * 
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> int4(String name, BiConsumer<R, Integer> setter) {
        set(name, TsurugiResultRecord::nextInt4OrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> int4(String name, BiConsumer<R, V> setter, Function<Integer, V> converter) {
        return int4(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // int8

    /**
     * add setter
     * 
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> int8(BiConsumer<R, Long> setter) {
        int index = columnConverterList.size();
        return int8(index, setter);
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> int8(BiConsumer<R, V> setter, Function<Long, V> converter) {
        int index = columnConverterList.size();
        return int8(index, setter, converter);
    }

    /**
     * add setter
     * 
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> int8(int index, BiConsumer<R, Long> setter) {
        set(index, TsurugiResultRecord::nextInt8OrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> int8(int index, BiConsumer<R, V> setter, Function<Long, V> converter) {
        return int8(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter
     * 
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> int8(String name, BiConsumer<R, Long> setter) {
        set(name, TsurugiResultRecord::nextInt8OrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> int8(String name, BiConsumer<R, V> setter, Function<Long, V> converter) {
        return int8(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // float4

    /**
     * add setter
     * 
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> float4(BiConsumer<R, Float> setter) {
        int index = columnConverterList.size();
        return float4(index, setter);
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> float4(BiConsumer<R, V> setter, Function<Float, V> converter) {
        int index = columnConverterList.size();
        return float4(index, setter, converter);
    }

    /**
     * add setter
     * 
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> float4(int index, BiConsumer<R, Float> setter) {
        set(index, TsurugiResultRecord::nextFloat4OrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> float4(int index, BiConsumer<R, V> setter, Function<Float, V> converter) {
        return float4(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter
     * 
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> float4(String name, BiConsumer<R, Float> setter) {
        set(name, TsurugiResultRecord::nextFloat4OrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> float4(String name, BiConsumer<R, V> setter, Function<Float, V> converter) {
        return float4(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // float8

    /**
     * add setter
     * 
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> float8(BiConsumer<R, Double> setter) {
        int index = columnConverterList.size();
        return float8(index, setter);
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> float8(BiConsumer<R, V> setter, Function<Double, V> converter) {
        int index = columnConverterList.size();
        return float8(index, setter, converter);
    }

    /**
     * add setter
     * 
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> float8(int index, BiConsumer<R, Double> setter) {
        set(index, TsurugiResultRecord::nextFloat8OrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> float8(int index, BiConsumer<R, V> setter, Function<Double, V> converter) {
        return float8(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter
     * 
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> float8(String name, BiConsumer<R, Double> setter) {
        set(name, TsurugiResultRecord::nextFloat8OrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> float8(String name, BiConsumer<R, V> setter, Function<Double, V> converter) {
        return float8(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // decimal

    /**
     * add setter
     * 
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> decimal(BiConsumer<R, BigDecimal> setter) {
        int index = columnConverterList.size();
        return decimal(index, setter);
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> decimal(BiConsumer<R, V> setter, Function<BigDecimal, V> converter) {
        int index = columnConverterList.size();
        return decimal(index, setter, converter);
    }

    /**
     * add setter
     * 
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> decimal(int index, BiConsumer<R, BigDecimal> setter) {
        set(index, TsurugiResultRecord::nextDecimalOrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> decimal(int index, BiConsumer<R, V> setter, Function<BigDecimal, V> converter) {
        return decimal(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter
     * 
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> decimal(String name, BiConsumer<R, BigDecimal> setter) {
        set(name, TsurugiResultRecord::nextDecimalOrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> decimal(String name, BiConsumer<R, V> setter, Function<BigDecimal, V> converter) {
        return decimal(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // character

    /**
     * add setter
     * 
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> character(BiConsumer<R, String> setter) {
        int index = columnConverterList.size();
        return character(index, setter);
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> character(BiConsumer<R, V> setter, Function<String, V> converter) {
        int index = columnConverterList.size();
        return character(index, setter, converter);
    }

    /**
     * add setter
     * 
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> character(int index, BiConsumer<R, String> setter) {
        set(index, TsurugiResultRecord::nextCharacterOrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> character(int index, BiConsumer<R, V> setter, Function<String, V> converter) {
        return character(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter
     * 
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> character(String name, BiConsumer<R, String> setter) {
        set(name, TsurugiResultRecord::nextCharacterOrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> character(String name, BiConsumer<R, V> setter, Function<String, V> converter) {
        return character(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // byte[]

    /**
     * add setter
     * 
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> bytes(BiConsumer<R, byte[]> setter) {
        int index = columnConverterList.size();
        return bytes(index, setter);
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> bytes(BiConsumer<R, V> setter, Function<byte[], V> converter) {
        int index = columnConverterList.size();
        return bytes(index, setter, converter);
    }

    /**
     * add setter
     * 
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> bytes(int index, BiConsumer<R, byte[]> setter) {
        set(index, TsurugiResultRecord::nextBytesOrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> bytes(int index, BiConsumer<R, V> setter, Function<byte[], V> converter) {
        return bytes(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter
     * 
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> bytes(String name, BiConsumer<R, byte[]> setter) {
        set(name, TsurugiResultRecord::nextBytesOrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> bytes(String name, BiConsumer<R, V> setter, Function<byte[], V> converter) {
        return bytes(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // boolean[]

    /**
     * add setter
     * 
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> bits(BiConsumer<R, boolean[]> setter) {
        int index = columnConverterList.size();
        return bits(index, setter);
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> bits(BiConsumer<R, V> setter, Function<boolean[], V> converter) {
        int index = columnConverterList.size();
        return bits(index, setter, converter);
    }

    /**
     * add setter
     * 
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> bits(int index, BiConsumer<R, boolean[]> setter) {
        set(index, TsurugiResultRecord::nextBitsOrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> bits(int index, BiConsumer<R, V> setter, Function<boolean[], V> converter) {
        return bits(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter
     * 
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> bits(String name, BiConsumer<R, boolean[]> setter) {
        set(name, TsurugiResultRecord::nextBitsOrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> bits(String name, BiConsumer<R, V> setter, Function<boolean[], V> converter) {
        return bits(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // date

    /**
     * add setter
     * 
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> date(BiConsumer<R, LocalDate> setter) {
        int index = columnConverterList.size();
        return date(index, setter);
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> date(BiConsumer<R, V> setter, Function<LocalDate, V> converter) {
        int index = columnConverterList.size();
        return date(index, setter, converter);
    }

    /**
     * add setter
     * 
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> date(int index, BiConsumer<R, LocalDate> setter) {
        set(index, TsurugiResultRecord::nextDateOrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> date(int index, BiConsumer<R, V> setter, Function<LocalDate, V> converter) {
        return date(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter
     * 
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> date(String name, BiConsumer<R, LocalDate> setter) {
        set(name, TsurugiResultRecord::nextDateOrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> date(String name, BiConsumer<R, V> setter, Function<LocalDate, V> converter) {
        return date(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // time

    /**
     * add setter
     * 
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> time(BiConsumer<R, LocalTime> setter) {
        int index = columnConverterList.size();
        return time(index, setter);
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> time(BiConsumer<R, V> setter, Function<LocalTime, V> converter) {
        int index = columnConverterList.size();
        return time(index, setter, converter);
    }

    /**
     * add setter
     * 
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> time(int index, BiConsumer<R, LocalTime> setter) {
        set(index, TsurugiResultRecord::nextTimeOrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> time(int index, BiConsumer<R, V> setter, Function<LocalTime, V> converter) {
        return time(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter
     * 
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> time(String name, BiConsumer<R, LocalTime> setter) {
        set(name, TsurugiResultRecord::nextTimeOrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> time(String name, BiConsumer<R, V> setter, Function<LocalTime, V> converter) {
        return time(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // instant

    /**
     * add setter
     * 
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> instant(BiConsumer<R, Instant> setter) {
        int index = columnConverterList.size();
        return instant(index, setter);
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> instant(BiConsumer<R, V> setter, Function<Instant, V> converter) {
        int index = columnConverterList.size();
        return instant(index, setter, converter);
    }

    /**
     * add setter
     * 
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> instant(int index, BiConsumer<R, Instant> setter) {
        set(index, TsurugiResultRecord::nextInstantOrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> instant(int index, BiConsumer<R, V> setter, Function<Instant, V> converter) {
        return instant(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter
     * 
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> instant(String name, BiConsumer<R, Instant> setter) {
        set(name, TsurugiResultRecord::nextInstantOrNull, setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> instant(String name, BiConsumer<R, V> setter, Function<Instant, V> converter) {
        return instant(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // ZonedDateTime

    /**
     * add setter
     * 
     * @param setter setter to R
     * @param zone   time-zone
     * @return this
     */
    public TgEntityResultMapping<R> zonedDateTime(BiConsumer<R, ZonedDateTime> setter, ZoneId zone) {
        int index = columnConverterList.size();
        return zonedDateTime(index, setter, zone);
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @param zone      time-zone
     * @return this
     */
    public <V> TgEntityResultMapping<R> zonedDateTime(BiConsumer<R, V> setter, Function<ZonedDateTime, V> converter, ZoneId zone) {
        int index = columnConverterList.size();
        return zonedDateTime(index, setter, converter, zone);
    }

    /**
     * add setter
     * 
     * @param index  column index
     * @param setter setter to R
     * @param zone   time-zone
     * @return this
     */
    public TgEntityResultMapping<R> zonedDateTime(int index, BiConsumer<R, ZonedDateTime> setter, ZoneId zone) {
        set(index, record -> record.nextZonedDateTimeOrNull(zone), setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @param zone      time-zone
     * @return this
     */
    public <V> TgEntityResultMapping<R> zonedDateTime(int index, BiConsumer<R, V> setter, Function<ZonedDateTime, V> converter, ZoneId zone) {
        return zonedDateTime(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        }, zone);
    }

    /**
     * add setter
     * 
     * @param name   column name
     * @param setter setter to R
     * @param zone   time-zone
     * @return this
     */
    public TgEntityResultMapping<R> zonedDateTime(String name, BiConsumer<R, ZonedDateTime> setter, ZoneId zone) {
        set(name, record -> record.nextZonedDateTimeOrNull(zone), setter);
        return this;
    }

    /**
     * add setter
     * 
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @param zone      time-zone
     * @return this
     */
    public <V> TgEntityResultMapping<R> zonedDateTime(String name, BiConsumer<R, V> setter, Function<ZonedDateTime, V> converter, ZoneId zone) {
        return zonedDateTime(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        }, zone);
    }

    // Object

    private static final Map<TgDataType, IoFunction<TsurugiResultRecord, Object>> RECORD_GETTER_MAP;
    static {
        var map = new EnumMap<TgDataType, IoFunction<TsurugiResultRecord, Object>>(TgDataType.class);
        map.put(TgDataType.BOOLEAN, TsurugiResultRecord::nextBooleanOrNull);
        map.put(TgDataType.INT4, TsurugiResultRecord::nextInt4OrNull);
        map.put(TgDataType.INT8, TsurugiResultRecord::nextInt8OrNull);
        map.put(TgDataType.FLOAT4, TsurugiResultRecord::nextFloat4OrNull);
        map.put(TgDataType.FLOAT8, TsurugiResultRecord::nextFloat8OrNull);
        map.put(TgDataType.DECIMAL, TsurugiResultRecord::nextDecimalOrNull);
        map.put(TgDataType.CHARACTER, TsurugiResultRecord::nextCharacterOrNull);
        map.put(TgDataType.BYTES, TsurugiResultRecord::nextBytesOrNull);
        map.put(TgDataType.BITS, TsurugiResultRecord::nextBitsOrNull);
        map.put(TgDataType.DATE, TsurugiResultRecord::nextDateOrNull);
        map.put(TgDataType.TIME, TsurugiResultRecord::nextTimeOrNull);
        map.put(TgDataType.INSTANT, TsurugiResultRecord::nextInstantOrNull);
        RECORD_GETTER_MAP = map;
    }

    /**
     * add setter
     * 
     * @param type   data type
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> add(TgDataType type, BiConsumer<R, Object> setter) {
        int index = columnConverterList.size();
        var recordGetter = RECORD_GETTER_MAP.get(type);
        if (recordGetter == null) {
            throw new UnsupportedOperationException("unsupported type error. type=" + type);
        }
        set(index, recordGetter, setter);
        return this;
    }

    /**
     * add converter
     * 
     * @param converter converter to R
     * @return this
     */
    public TgEntityResultMapping<R> add(IoBiConsumer<R, TsurugiResultRecord> converter) {
        int index = columnConverterList.size();
        set(index, converter);
        return this;
    }

    protected <V> void set(int index, IoFunction<TsurugiResultRecord, V> recordGetter, BiConsumer<R, V> setter) {
        set(index, (entity, record) -> {
            V value = recordGetter.apply(record);
            setter.accept(entity, value);
        });
    }

    protected void set(int index, IoBiConsumer<R, TsurugiResultRecord> converter) {
        while (index >= columnConverterList.size()) {
            columnConverterList.add(null);
        }
        columnConverterList.set(index, converter);
    }

    protected <V> void set(String name, IoFunction<TsurugiResultRecord, V> recordGetter, BiConsumer<R, V> setter) {
        set(name, (entity, record) -> {
            V value = recordGetter.apply(record);
            setter.accept(entity, value);
        });
    }

    protected void set(String name, IoBiConsumer<R, TsurugiResultRecord> converter) {
        if (this.columnConverterMap == null) {
            this.columnConverterMap = new HashMap<>();
        }
        columnConverterMap.put(name, converter);
    }

//  @ThreadSafe
    @Override
    protected R convert(TsurugiResultRecord record) throws IOException {
        mergeColumnConverterMap(record);

        R entity = entitySupplier.get();
        for (var converter : columnConverterList) {
            if (converter != null) {
                converter.accept(entity, record);
            } else {
                record.nextColumn();
            }
        }
        return entity;
    }

    protected synchronized void mergeColumnConverterMap(TsurugiResultRecord record) throws IOException {
        if (this.columnConverterMap != null) {
            var nameList = record.getNameList();
            int i = 0;
            for (var name : nameList) {
                var converter = columnConverterMap.get(name);
                if (converter != null) {
                    set(i, converter);
                }
                i++;
            }
            this.columnConverterMap = null;
        }
    }
}
