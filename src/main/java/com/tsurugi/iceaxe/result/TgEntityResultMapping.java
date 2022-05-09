package com.tsurugi.iceaxe.result;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import com.tsurugi.iceaxe.statement.TgDataType;
import com.tsurugi.iceaxe.util.IoBiConsumer;

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
     * @param index  column index
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> int4(int index, BiConsumer<R, Integer> setter) {
        set(index, (entity, record) -> {
            var value = record.nextInt4OrNull();
            setter.accept(entity, value);
        });
        return this;
    }

    /**
     * add setter
     * 
     * @param name   colum name
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> int4(String name, BiConsumer<R, Integer> setter) {
        set(name, (entity, record) -> {
            var value = record.nextInt4OrNull();
            setter.accept(entity, value);
        });
        return this;
    }

    // TODO int8, float4, float8, character

    /**
     * add setter
     * 
     * @param type   data type
     * @param setter setter to R
     * @return this
     */
    public TgEntityResultMapping<R> add(TgDataType type, BiConsumer<R, Object> setter) {
        int index = columnConverterList.size();
        switch (type) {
        case INT4:
            set(index, (entity, record) -> {
                var value = record.nextInt4OrNull();
                setter.accept(entity, value);
            });
            break;
        // TODO int8, float4, float8, character
        default:
            throw new InternalError("not yet implements. type=" + type);
        }
        return this;
    }

    protected void set(int index, IoBiConsumer<R, TsurugiResultRecord> converter) {
        while (index >= columnConverterList.size()) {
            columnConverterList.add(null);
        }
        columnConverterList.set(index, converter);
    }

    protected void set(String name, IoBiConsumer<R, TsurugiResultRecord> converter) {
        if (this.columnConverterMap == null) {
            this.columnConverterMap = new HashMap<>();
        }
        columnConverterMap.put(name, converter);
    }

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
            for (int i = 0; i < nameList.size(); i++) {
                var name = nameList.get(i);
                var converter = columnConverterMap.get(name);
                if (converter != null) {
                    set(i, converter);
                }
            }
            this.columnConverterMap = null;
        }
    }
}
