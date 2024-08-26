package com.tsurugidb.iceaxe.sql.result.mapping;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
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

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.iceaxe.util.function.TsurugiTransactionBiConsumer;
import com.tsurugidb.iceaxe.util.function.TsurugiTransactionFunction;

/**
 * Tsurugi Result Mapping for Entity.
 *
 * @param <R> result type (e.g. Entity)
 */
public class TgEntityResultMapping<R> extends TgResultMapping<R> {

    /**
     * create Result Mapping.
     *
     * @param <R>            result type
     * @param entitySupplier supplier of R
     * @return Result Mapping
     */
    public static <R> TgEntityResultMapping<R> of(Supplier<R> entitySupplier) {
        return new TgEntityResultMapping<R>().setSupplier(entitySupplier);
    }

    private Supplier<R> entitySupplier;
    private final List<TsurugiTransactionBiConsumer<R, TsurugiResultRecord>> columnConverterList = new ArrayList<>();

    private static /* record */ class NameConverter<R> {
        private final String name;
        private final TsurugiTransactionBiConsumer<R, TsurugiResultRecord> converter;

        public NameConverter(String name, TsurugiTransactionBiConsumer<R, TsurugiResultRecord> converter) {
            this.name = name;
            this.converter = converter;
        }

        public String name() {
            return this.name;
        }

        public TsurugiTransactionBiConsumer<R, TsurugiResultRecord> converter() {
            return this.converter;
        }
    }

    private List<NameConverter<R>> nameConverterList = null;

    /**
     * Tsurugi Result Mapping.
     */
    public TgEntityResultMapping() {
        // do nothing
    }

    /**
     * set supplier.
     *
     * @param entitySupplier supplier of R
     * @return this
     */
    public TgEntityResultMapping<R> setSupplier(Supplier<R> entitySupplier) {
        this.entitySupplier = entitySupplier;
        return this;
    }

    @Override
    public TgEntityResultMapping<R> setConvertUtil(IceaxeConvertUtil convertUtil) {
        return (TgEntityResultMapping<R>) super.setConvertUtil(convertUtil);
    }

    // boolean

    /**
     * add setter.
     *
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addBoolean(BiConsumer<R, Boolean> setter) {
        int index = columnConverterList.size();
        return addBoolean(index, setter);
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addBoolean(BiConsumer<R, V> setter, Function<Boolean, V> converter) {
        int index = columnConverterList.size();
        return addBoolean(index, setter, converter);
    }

    /**
     * add setter.
     *
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addBoolean(int index, BiConsumer<R, Boolean> setter) {
        set(index, TsurugiResultRecord::nextBooleanOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addBoolean(int index, BiConsumer<R, V> setter, Function<Boolean, V> converter) {
        return addBoolean(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter.
     *
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addBoolean(String name, BiConsumer<R, Boolean> setter) {
        set(name, TsurugiResultRecord::nextBooleanOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addBoolean(String name, BiConsumer<R, V> setter, Function<Boolean, V> converter) {
        return addBoolean(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // int

    /**
     * add setter.
     *
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addInt(BiConsumer<R, Integer> setter) {
        int index = columnConverterList.size();
        return addInt(index, setter);
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addInt(BiConsumer<R, V> setter, Function<Integer, V> converter) {
        int index = columnConverterList.size();
        return addInt(index, setter, converter);
    }

    /**
     * add setter.
     *
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addInt(int index, BiConsumer<R, Integer> setter) {
        set(index, TsurugiResultRecord::nextIntOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addInt(int index, BiConsumer<R, V> setter, Function<Integer, V> converter) {
        return addInt(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter.
     *
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addInt(String name, BiConsumer<R, Integer> setter) {
        set(name, TsurugiResultRecord::nextIntOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addInt(String name, BiConsumer<R, V> setter, Function<Integer, V> converter) {
        return addInt(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // long

    /**
     * add setter.
     *
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addLong(BiConsumer<R, Long> setter) {
        int index = columnConverterList.size();
        return addLong(index, setter);
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addLong(BiConsumer<R, V> setter, Function<Long, V> converter) {
        int index = columnConverterList.size();
        return addLong(index, setter, converter);
    }

    /**
     * add setter.
     *
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addLong(int index, BiConsumer<R, Long> setter) {
        set(index, TsurugiResultRecord::nextLongOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addLong(int index, BiConsumer<R, V> setter, Function<Long, V> converter) {
        return addLong(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter.
     *
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addLong(String name, BiConsumer<R, Long> setter) {
        set(name, TsurugiResultRecord::nextLongOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addLong(String name, BiConsumer<R, V> setter, Function<Long, V> converter) {
        return addLong(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // float

    /**
     * add setter.
     *
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addFloat(BiConsumer<R, Float> setter) {
        int index = columnConverterList.size();
        return addFloat(index, setter);
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addFloat(BiConsumer<R, V> setter, Function<Float, V> converter) {
        int index = columnConverterList.size();
        return addFloat(index, setter, converter);
    }

    /**
     * add setter.
     *
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addFloat(int index, BiConsumer<R, Float> setter) {
        set(index, TsurugiResultRecord::nextFloatOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addFloat(int index, BiConsumer<R, V> setter, Function<Float, V> converter) {
        return addFloat(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter.
     *
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addFloat(String name, BiConsumer<R, Float> setter) {
        set(name, TsurugiResultRecord::nextFloatOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addFloat(String name, BiConsumer<R, V> setter, Function<Float, V> converter) {
        return addFloat(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // double

    /**
     * add setter.
     *
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addDouble(BiConsumer<R, Double> setter) {
        int index = columnConverterList.size();
        return addDouble(index, setter);
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addDouble(BiConsumer<R, V> setter, Function<Double, V> converter) {
        int index = columnConverterList.size();
        return addDouble(index, setter, converter);
    }

    /**
     * add setter.
     *
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addDouble(int index, BiConsumer<R, Double> setter) {
        set(index, TsurugiResultRecord::nextDoubleOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addDouble(int index, BiConsumer<R, V> setter, Function<Double, V> converter) {
        return addDouble(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter.
     *
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addDouble(String name, BiConsumer<R, Double> setter) {
        set(name, TsurugiResultRecord::nextDoubleOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addDouble(String name, BiConsumer<R, V> setter, Function<Double, V> converter) {
        return addDouble(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // decimal

    /**
     * add setter.
     *
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addDecimal(BiConsumer<R, BigDecimal> setter) {
        int index = columnConverterList.size();
        return addDecimal(index, setter);
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addDecimal(BiConsumer<R, V> setter, Function<BigDecimal, V> converter) {
        int index = columnConverterList.size();
        return addDecimal(index, setter, converter);
    }

    /**
     * add setter.
     *
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addDecimal(int index, BiConsumer<R, BigDecimal> setter) {
        set(index, TsurugiResultRecord::nextDecimalOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addDecimal(int index, BiConsumer<R, V> setter, Function<BigDecimal, V> converter) {
        return addDecimal(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter.
     *
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addDecimal(String name, BiConsumer<R, BigDecimal> setter) {
        set(name, TsurugiResultRecord::nextDecimalOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addDecimal(String name, BiConsumer<R, V> setter, Function<BigDecimal, V> converter) {
        return addDecimal(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // string

    /**
     * add setter.
     *
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addString(BiConsumer<R, String> setter) {
        int index = columnConverterList.size();
        return addString(index, setter);
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addString(BiConsumer<R, V> setter, Function<String, V> converter) {
        int index = columnConverterList.size();
        return addString(index, setter, converter);
    }

    /**
     * add setter.
     *
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addString(int index, BiConsumer<R, String> setter) {
        set(index, TsurugiResultRecord::nextStringOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addString(int index, BiConsumer<R, V> setter, Function<String, V> converter) {
        return addString(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter.
     *
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addString(String name, BiConsumer<R, String> setter) {
        set(name, TsurugiResultRecord::nextStringOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addString(String name, BiConsumer<R, V> setter, Function<String, V> converter) {
        return addString(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // byte[]

    /**
     * add setter.
     *
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addBytes(BiConsumer<R, byte[]> setter) {
        int index = columnConverterList.size();
        return addBytes(index, setter);
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addBytes(BiConsumer<R, V> setter, Function<byte[], V> converter) {
        int index = columnConverterList.size();
        return addBytes(index, setter, converter);
    }

    /**
     * add setter.
     *
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addBytes(int index, BiConsumer<R, byte[]> setter) {
        set(index, TsurugiResultRecord::nextBytesOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addBytes(int index, BiConsumer<R, V> setter, Function<byte[], V> converter) {
        return addBytes(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter.
     *
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addBytes(String name, BiConsumer<R, byte[]> setter) {
        set(name, TsurugiResultRecord::nextBytesOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addBytes(String name, BiConsumer<R, V> setter, Function<byte[], V> converter) {
        return addBytes(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // boolean[]

    /**
     * <em>This method is not yet implemented:</em>
     * add setter.
     *
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addBits(BiConsumer<R, boolean[]> setter) {
        int index = columnConverterList.size();
        return addBits(index, setter);
    }

    /**
     * <em>This method is not yet implemented:</em>
     * add setter.
     *
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addBits(BiConsumer<R, V> setter, Function<boolean[], V> converter) {
        int index = columnConverterList.size();
        return addBits(index, setter, converter);
    }

    /**
     * <em>This method is not yet implemented:</em>
     * add setter.
     *
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addBits(int index, BiConsumer<R, boolean[]> setter) {
        set(index, TsurugiResultRecord::nextBitsOrNull, setter);
        return this;
    }

    /**
     * <em>This method is not yet implemented:</em>
     * add setter.
     *
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addBits(int index, BiConsumer<R, V> setter, Function<boolean[], V> converter) {
        return addBits(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * <em>This method is not yet implemented:</em>
     * add setter.
     *
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addBits(String name, BiConsumer<R, boolean[]> setter) {
        set(name, TsurugiResultRecord::nextBitsOrNull, setter);
        return this;
    }

    /**
     * <em>This method is not yet implemented:</em>
     * add setter.
     *
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addBits(String name, BiConsumer<R, V> setter, Function<boolean[], V> converter) {
        return addBits(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // date

    /**
     * add setter.
     *
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addDate(BiConsumer<R, LocalDate> setter) {
        int index = columnConverterList.size();
        return addDate(index, setter);
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addDate(BiConsumer<R, V> setter, Function<LocalDate, V> converter) {
        int index = columnConverterList.size();
        return addDate(index, setter, converter);
    }

    /**
     * add setter.
     *
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addDate(int index, BiConsumer<R, LocalDate> setter) {
        set(index, TsurugiResultRecord::nextDateOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addDate(int index, BiConsumer<R, V> setter, Function<LocalDate, V> converter) {
        return addDate(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter.
     *
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addDate(String name, BiConsumer<R, LocalDate> setter) {
        set(name, TsurugiResultRecord::nextDateOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addDate(String name, BiConsumer<R, V> setter, Function<LocalDate, V> converter) {
        return addDate(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // time

    /**
     * add setter.
     *
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addTime(BiConsumer<R, LocalTime> setter) {
        int index = columnConverterList.size();
        return addTime(index, setter);
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addTime(BiConsumer<R, V> setter, Function<LocalTime, V> converter) {
        int index = columnConverterList.size();
        return addTime(index, setter, converter);
    }

    /**
     * add setter.
     *
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addTime(int index, BiConsumer<R, LocalTime> setter) {
        set(index, TsurugiResultRecord::nextTimeOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addTime(int index, BiConsumer<R, V> setter, Function<LocalTime, V> converter) {
        return addTime(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter.
     *
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addTime(String name, BiConsumer<R, LocalTime> setter) {
        set(name, TsurugiResultRecord::nextTimeOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addTime(String name, BiConsumer<R, V> setter, Function<LocalTime, V> converter) {
        return addTime(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // dateTime

    /**
     * add setter.
     *
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addDateTime(BiConsumer<R, LocalDateTime> setter) {
        int index = columnConverterList.size();
        return addDateTime(index, setter);
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addDateTime(BiConsumer<R, V> setter, Function<LocalDateTime, V> converter) {
        int index = columnConverterList.size();
        return addDateTime(index, setter, converter);
    }

    /**
     * add setter.
     *
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addDateTime(int index, BiConsumer<R, LocalDateTime> setter) {
        set(index, TsurugiResultRecord::nextDateTimeOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addDateTime(int index, BiConsumer<R, V> setter, Function<LocalDateTime, V> converter) {
        return addDateTime(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter.
     *
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addDateTime(String name, BiConsumer<R, LocalDateTime> setter) {
        set(name, TsurugiResultRecord::nextDateTimeOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addDateTime(String name, BiConsumer<R, V> setter, Function<LocalDateTime, V> converter) {
        return addDateTime(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // offset time

    /**
     * add setter.
     *
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addOffsetTime(BiConsumer<R, OffsetTime> setter) {
        int index = columnConverterList.size();
        return addOffsetTime(index, setter);
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addOffsetTime(BiConsumer<R, V> setter, Function<OffsetTime, V> converter) {
        int index = columnConverterList.size();
        return addOffsetTime(index, setter, converter);
    }

    /**
     * add setter.
     *
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addOffsetTime(int index, BiConsumer<R, OffsetTime> setter) {
        set(index, TsurugiResultRecord::nextOffsetTimeOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addOffsetTime(int index, BiConsumer<R, V> setter, Function<OffsetTime, V> converter) {
        return addOffsetTime(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter.
     *
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addOffsetTime(String name, BiConsumer<R, OffsetTime> setter) {
        set(name, TsurugiResultRecord::nextOffsetTimeOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addOffsetTime(String name, BiConsumer<R, V> setter, Function<OffsetTime, V> converter) {
        return addOffsetTime(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // offset dateTime

    /**
     * add setter.
     *
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addOffsetDateTime(BiConsumer<R, OffsetDateTime> setter) {
        int index = columnConverterList.size();
        return addOffsetDateTime(index, setter);
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addOffsetDateTime(BiConsumer<R, V> setter, Function<OffsetDateTime, V> converter) {
        int index = columnConverterList.size();
        return addOffsetDateTime(index, setter, converter);
    }

    /**
     * add setter.
     *
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addOffsetDateTime(int index, BiConsumer<R, OffsetDateTime> setter) {
        set(index, TsurugiResultRecord::nextOffsetDateTimeOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addOffsetDateTime(int index, BiConsumer<R, V> setter, Function<OffsetDateTime, V> converter) {
        return addOffsetDateTime(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    /**
     * add setter.
     *
     * @param name   column name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> addOffsetDateTime(String name, BiConsumer<R, OffsetDateTime> setter) {
        set(name, TsurugiResultRecord::nextOffsetDateTimeOrNull, setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @return this
     */
    public <V> TgEntityResultMapping<R> addOffsetDateTime(String name, BiConsumer<R, V> setter, Function<OffsetDateTime, V> converter) {
        return addOffsetDateTime(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        });
    }

    // zoned dateTime

    /**
     * add setter.
     *
     * @param setter setter to R
     * @param zone   time-zone
     * @return this
     */
    public TgEntityResultMapping<R> addZonedDateTime(BiConsumer<R, ZonedDateTime> setter, ZoneId zone) {
        int index = columnConverterList.size();
        return addZonedDateTime(index, setter, zone);
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param setter    setter to R
     * @param converter converter to V
     * @param zone      time-zone
     * @return this
     */
    public <V> TgEntityResultMapping<R> addZonedDateTime(BiConsumer<R, V> setter, Function<ZonedDateTime, V> converter, ZoneId zone) {
        int index = columnConverterList.size();
        return addZonedDateTime(index, setter, converter, zone);
    }

    /**
     * add setter.
     *
     * @param index  column index
     * @param setter setter to R
     * @param zone   time-zone
     * @return this
     */
    public TgEntityResultMapping<R> addZonedDateTime(int index, BiConsumer<R, ZonedDateTime> setter, ZoneId zone) {
        set(index, record -> record.nextZonedDateTimeOrNull(zone), setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param index     column index
     * @param setter    setter to R
     * @param converter converter to V
     * @param zone      time-zone
     * @return this
     */
    public <V> TgEntityResultMapping<R> addZonedDateTime(int index, BiConsumer<R, V> setter, Function<ZonedDateTime, V> converter, ZoneId zone) {
        return addZonedDateTime(index, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        }, zone);
    }

    /**
     * add setter.
     *
     * @param name   column name
     * @param setter setter to R
     * @param zone   time-zone
     * @return this
     */
    public TgEntityResultMapping<R> addZonedDateTime(String name, BiConsumer<R, ZonedDateTime> setter, ZoneId zone) {
        set(name, record -> record.nextZonedDateTimeOrNull(zone), setter);
        return this;
    }

    /**
     * add setter.
     *
     * @param <V>       value type
     * @param name      column name
     * @param setter    setter to R
     * @param converter converter to V
     * @param zone      time-zone
     * @return this
     */
    public <V> TgEntityResultMapping<R> addZonedDateTime(String name, BiConsumer<R, V> setter, Function<ZonedDateTime, V> converter, ZoneId zone) {
        return addZonedDateTime(name, (entity, value) -> {
            V v = (value != null) ? converter.apply(value) : null;
            setter.accept(entity, v);
        }, zone);
    }

    // Object

    private static final Map<TgDataType, TsurugiTransactionFunction<TsurugiResultRecord, Object>> RECORD_GETTER_MAP;
    static {
        var map = new EnumMap<TgDataType, TsurugiTransactionFunction<TsurugiResultRecord, Object>>(TgDataType.class);
        map.put(TgDataType.BOOLEAN, TsurugiResultRecord::nextBooleanOrNull);
        map.put(TgDataType.INT, TsurugiResultRecord::nextIntOrNull);
        map.put(TgDataType.LONG, TsurugiResultRecord::nextLongOrNull);
        map.put(TgDataType.FLOAT, TsurugiResultRecord::nextFloatOrNull);
        map.put(TgDataType.DOUBLE, TsurugiResultRecord::nextDoubleOrNull);
        map.put(TgDataType.DECIMAL, TsurugiResultRecord::nextDecimalOrNull);
        map.put(TgDataType.STRING, TsurugiResultRecord::nextStringOrNull);
        map.put(TgDataType.BYTES, TsurugiResultRecord::nextBytesOrNull);
        map.put(TgDataType.BITS, TsurugiResultRecord::nextBitsOrNull);
        map.put(TgDataType.DATE, TsurugiResultRecord::nextDateOrNull);
        map.put(TgDataType.TIME, TsurugiResultRecord::nextTimeOrNull);
        map.put(TgDataType.DATE_TIME, TsurugiResultRecord::nextDateTimeOrNull);
        map.put(TgDataType.OFFSET_TIME, TsurugiResultRecord::nextOffsetTimeOrNull);
        map.put(TgDataType.OFFSET_DATE_TIME, TsurugiResultRecord::nextOffsetDateTimeOrNull);
//x     map.put(TgDataType.ZONED_DATE_TIME, TsurugiResultRecord::nextZonedDateTimeOrNull);
        RECORD_GETTER_MAP = map;
    }

    /**
     * add setter.
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
     * add converter.
     *
     * @param converter converter to R
     * @return this
     */
    public TgEntityResultMapping<R> add(TsurugiTransactionBiConsumer<R, TsurugiResultRecord> converter) {
        int index = columnConverterList.size();
        set(index, converter);
        return this;
    }

    /**
     * set getter and setter.
     *
     * @param <V>          data type
     * @param index        column index
     * @param recordGetter getter from record
     * @param setter       setter
     */
    protected <V> void set(int index, TsurugiTransactionFunction<TsurugiResultRecord, V> recordGetter, BiConsumer<R, V> setter) {
        set(index, (entity, record) -> {
            V value = recordGetter.apply(record);
            setter.accept(entity, value);
        });
    }

    /**
     * set converter.
     *
     * @param index     column index
     * @param converter converter to R
     */
    protected void set(int index, TsurugiTransactionBiConsumer<R, TsurugiResultRecord> converter) {
        while (index >= columnConverterList.size()) {
            columnConverterList.add(null);
        }
        columnConverterList.set(index, converter);
    }

    /**
     * set getter and setter.
     *
     * @param <V>          data type
     * @param name         column name
     * @param recordGetter getter from record
     * @param setter       setter
     */
    protected <V> void set(String name, TsurugiTransactionFunction<TsurugiResultRecord, V> recordGetter, BiConsumer<R, V> setter) {
        set(name, (entity, record) -> {
            V value = recordGetter.apply(record);
            setter.accept(entity, value);
        });
    }

    /**
     * set converter.
     *
     * @param name      column name
     * @param converter converter to R
     */
    protected void set(String name, TsurugiTransactionBiConsumer<R, TsurugiResultRecord> converter) {
        if (this.nameConverterList == null) {
            this.nameConverterList = new ArrayList<>();
        }

        var entry = new NameConverter<>(name, converter);
        nameConverterList.add(entry);
    }

//  @ThreadSafe
    @Override
    protected R convert(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException {
        mergeNameConverterList(record);

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

    /**
     * merge nameConverterList.
     *
     * @param record record
     * @throws IOException                 if an I/O error occurs while retrieving metadata
     * @throws InterruptedException        if interrupted while retrieving metadata
     * @throws TsurugiTransactionException if server error occurs while retrieving metadata
     */
    protected synchronized void mergeNameConverterList(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException {
        if (this.nameConverterList != null) {
            var nameList = record.getResultNameList();

            var countMap = new HashMap<String, int[]>(nameList.size());
            for (var entry : nameConverterList) {
                String name = entry.name();
                int[] counter = countMap.computeIfAbsent(name, k -> new int[] { 0 });
                int subIndex = counter[0]++;
                int index = nameList.getIndex(name, subIndex);

                var converter = entry.converter();
                set(index, converter);
            }

            this.nameConverterList = null;
        }
    }
}
