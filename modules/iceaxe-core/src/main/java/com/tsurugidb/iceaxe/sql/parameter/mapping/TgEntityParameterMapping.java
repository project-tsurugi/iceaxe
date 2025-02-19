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
package com.tsurugidb.iceaxe.sql.parameter.mapping;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.IceaxeLowParameterUtil;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.type.TgBlob;
import com.tsurugidb.iceaxe.util.IceaxeCloseableSet;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.sql.proto.SqlRequest.Placeholder;
import com.tsurugidb.tsubakuro.sql.Placeholders;;

/**
 * Tsurugi Parameter Mapping for Entity.
 *
 * @param <P> parameter type (e.g. Entity)
 */
public class TgEntityParameterMapping<P> extends TgParameterMapping<P> {

    /**
     * create Parameter Mapping.
     *
     * @param <P> parameter type
     * @return Tsurugi Parameter Mapping
     */
    public static <P> TgEntityParameterMapping<P> of() {
        return new TgEntityParameterMapping<>();
    }

    /**
     * create Parameter Mapping.
     *
     * @param <P>   parameter type
     * @param clazz parameter class
     * @return Tsurugi Parameter Mapping
     */
    public static <P> TgEntityParameterMapping<P> of(Class<P> clazz) {
        return new TgEntityParameterMapping<>();
    }

    private final List<Placeholder> lowPlaceholderList = new ArrayList<>();

    @FunctionalInterface
    private interface LowParameterGenerator<P> {
        Parameter apply(P parameter, IceaxeConvertUtil convertUtil, IceaxeCloseableSet closeableSet) throws IOException;
    }

    private final List<LowParameterGenerator<P>> parameterConverterList = new ArrayList<>();

    /**
     * Creates a new instance.
     */
    public TgEntityParameterMapping() {
        // do nothing
    }

    @Override
    public TgEntityParameterMapping<P> setConvertUtil(IceaxeConvertUtil convertUtil) {
        return (TgEntityParameterMapping<P>) super.setConvertUtil(convertUtil);
    }

    /**
     * add variable.
     *
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> addBoolean(String name, Function<P, Boolean> getter) {
        addVariable(name, TgDataType.BOOLEAN);
        parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable.
     *
     * @param <V>       value type
     * @param name      name
     * @param getter    getter from parameter
     * @param converter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> addBoolean(String name, Function<P, V> getter, Function<V, Boolean> converter) {
        return addBoolean(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable.
     *
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> addInt(String name, Function<P, Integer> getter) {
        addVariable(name, TgDataType.INT);
        parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable.
     *
     * @param <V>       value type
     * @param name      name
     * @param getter    getter from parameter
     * @param converter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> addInt(String name, Function<P, V> getter, Function<V, Integer> converter) {
        return addInt(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable.
     *
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> addLong(String name, Function<P, Long> getter) {
        addVariable(name, TgDataType.LONG);
        parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable.
     *
     * @param <V>       value type
     * @param name      name
     * @param getter    getter from parameter
     * @param converter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> addLong(String name, Function<P, V> getter, Function<V, Long> converter) {
        return addLong(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable.
     *
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> addFloat(String name, Function<P, Float> getter) {
        addVariable(name, TgDataType.FLOAT);
        parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable.
     *
     * @param <V>       value type
     * @param name      name
     * @param getter    getter from parameter
     * @param converter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> addFloat(String name, Function<P, V> getter, Function<V, Float> converter) {
        return addFloat(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable.
     *
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> addDouble(String name, Function<P, Double> getter) {
        addVariable(name, TgDataType.DOUBLE);
        parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable.
     *
     * @param <V>       value type
     * @param name      name
     * @param getter    getter from parameter
     * @param converter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> addDouble(String name, Function<P, V> getter, Function<V, Double> converter) {
        return addDouble(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable.
     *
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> addDecimal(String name, Function<P, BigDecimal> getter) {
        addVariable(name, TgDataType.DECIMAL);
        parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable.
     *
     * @param <V>       value type
     * @param name      name
     * @param getter    getter from parameter
     * @param converter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> addDecimal(String name, Function<P, V> getter, Function<V, BigDecimal> converter) {
        return addDecimal(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable.
     *
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> addString(String name, Function<P, String> getter) {
        addVariable(name, TgDataType.STRING);
        parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable.
     *
     * @param <V>       value type
     * @param name      name
     * @param getter    getter from parameter
     * @param converter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> addString(String name, Function<P, V> getter, Function<V, String> converter) {
        return addString(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable.
     *
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> addBytes(String name, Function<P, byte[]> getter) {
        addVariable(name, TgDataType.BYTES);
        parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable.
     *
     * @param <V>       value type
     * @param name      name
     * @param getter    getter from parameter
     * @param converter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> addBytes(String name, Function<P, V> getter, Function<V, byte[]> converter) {
        return addBytes(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * <em>This method is not yet implemented:</em> add variable.
     *
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> addBits(String name, Function<P, boolean[]> getter) {
        addVariable(name, TgDataType.BITS);
        parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * <em>This method is not yet implemented:</em> add variable.
     *
     * @param <V>       value type
     * @param name      name
     * @param getter    getter from parameter
     * @param converter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> addBits(String name, Function<P, V> getter, Function<V, boolean[]> converter) {
        return addBits(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable.
     *
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> addDate(String name, Function<P, LocalDate> getter) {
        addVariable(name, TgDataType.DATE);
        parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable.
     *
     * @param <V>       value type
     * @param name      name
     * @param getter    getter from parameter
     * @param converter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> addDate(String name, Function<P, V> getter, Function<V, LocalDate> converter) {
        return addDate(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable.
     *
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> addTime(String name, Function<P, LocalTime> getter) {
        addVariable(name, TgDataType.DATE);
        parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable.
     *
     * @param <V>       value type
     * @param name      name
     * @param getter    getter from parameter
     * @param converter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> addTime(String name, Function<P, V> getter, Function<V, LocalTime> converter) {
        return addTime(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable.
     *
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> addOffsetTime(String name, Function<P, OffsetTime> getter) {
        addVariable(name, TgDataType.OFFSET_TIME);
        parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable.
     *
     * @param <V>       value type
     * @param name      name
     * @param getter    getter from parameter
     * @param converter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> addOffsetTime(String name, Function<P, V> getter, Function<V, OffsetTime> converter) {
        return addOffsetTime(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable.
     *
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> addDateTime(String name, Function<P, LocalDateTime> getter) {
        addVariable(name, TgDataType.DATE_TIME);
        parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable.
     *
     * @param <V>       value type
     * @param name      name
     * @param getter    getter from parameter
     * @param converter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> addDateTime(String name, Function<P, V> getter, Function<V, LocalDateTime> converter) {
        return addDateTime(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable.
     *
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> addOffsetDateTime(String name, Function<P, OffsetDateTime> getter) {
        addVariable(name, TgDataType.OFFSET_DATE_TIME);
        parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable.
     *
     * @param <V>       value type
     * @param name      name
     * @param getter    getter from parameter
     * @param converter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> addOffsetDateTime(String name, Function<P, V> getter, Function<V, OffsetDateTime> converter) {
        return addOffsetDateTime(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable.
     *
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> addZonedDateTime(String name, Function<P, ZonedDateTime> getter) {
        addVariable(name, TgDataType.ZONED_DATE_TIME);
        parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable.
     *
     * @param <V>       value type
     * @param name      name
     * @param getter    getter from parameter
     * @param converter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> addZonedDateTime(String name, Function<P, V> getter, Function<V, ZonedDateTime> converter) {
        return addZonedDateTime(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable.
     *
     * @param name   name
     * @param getter getter from parameter
     * @return this
     * @since X.X.X
     */
    public TgEntityParameterMapping<P> addBlob(String name, Function<P, TgBlob> getter) {
        addVariable(name, TgDataType.BLOB);
        parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
            var value = getter.apply(parameter);
            if (value.isDeleteOnExecuteFinished()) {
                closeableSet.add(value);
            }
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable.
     *
     * @param <V>       value type
     * @param name      name
     * @param getter    getter from parameter
     * @param converter converter to database data type
     * @return this
     * @since X.X.X
     */
    public <V> TgEntityParameterMapping<P> addBlob(String name, Function<P, V> getter, Function<V, TgBlob> converter) {
        return addBlob(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable.
     *
     * @param name   name
     * @param getter getter from parameter
     * @return this
     * @since X.X.X
     */
    public TgEntityParameterMapping<P> addBlobPath(String name, Function<P, Path> getter) {
        addVariable(name, TgDataType.BLOB);
        parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.createBlob(name, value);
        });
        return this;
    }

    /**
     * add variable.
     *
     * @param <V>       value type
     * @param name      name
     * @param getter    getter from parameter
     * @param converter converter to database data type
     * @return this
     * @since X.X.X
     */
    public <V> TgEntityParameterMapping<P> addBlobPath(String name, Function<P, V> getter, Function<V, Path> converter) {
        return addBlobPath(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable.
     *
     * @param name   name
     * @param getter getter from parameter
     * @return this
     * @since X.X.X
     */
    public TgEntityParameterMapping<P> addBlobBytes(String name, Function<P, byte[]> getter) {
        addVariable(name, TgDataType.BLOB);
        parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
            var value = getter.apply(parameter);
            var factory = convertUtil.getIceaxeObjectFactory();
            var blob = factory.createBlob(value, true);
            closeableSet.add(blob);
            return IceaxeLowParameterUtil.create(name, blob);
        });
        return this;
    }

    // x addBlobBytes(String name, Function<P, V> getter, Function<V, byte[]> converter)

    /**
     * add variable.
     *
     * @param name   name
     * @param type   type
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> add(String name, TgDataType type, Function<P, ?> getter) {
        addVariable(name, type);
        switch (type) {
        case BOOLEAN:
            parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
                var value = convertUtil.toBoolean(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case INT:
            parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
                var value = convertUtil.toInt(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case LONG:
            parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
                var value = convertUtil.toLong(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case FLOAT:
            parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
                var value = convertUtil.toFloat(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case DOUBLE:
            parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
                var value = convertUtil.toDouble(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case DECIMAL:
            parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
                var value = convertUtil.toDecimal(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case STRING:
            parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
                var value = convertUtil.toString(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case BYTES:
            parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
                var value = convertUtil.toBytes(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case BITS:
            parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
                var value = convertUtil.toBits(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case DATE:
            parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
                var value = convertUtil.toDate(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case TIME:
            parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
                var value = convertUtil.toTime(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case DATE_TIME:
            parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
                var value = convertUtil.toDateTime(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case OFFSET_TIME:
            parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
                var value = convertUtil.toOffsetTime(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case OFFSET_DATE_TIME:
            parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
                var value = convertUtil.toOffsetDateTime(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case BLOB:
            parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
                var value = convertUtil.toBlob(getter.apply(parameter));
                if (value.isDeleteOnExecuteFinished()) {
                    closeableSet.add(value);
                }
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        // TODO CLOB
        case ZONED_DATE_TIME:
        default:
            throw new UnsupportedOperationException("unsupported type error. type=" + type);
        }
    }

    /**
     * add variable.
     *
     * @param <V>       value type
     * @param name      name
     * @param type      type
     * @param getter    getter from parameter
     * @param converter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> add(String name, TgDataType type, Function<P, V> getter, Function<V, ?> converter) {
        return add(name, type, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable.
     *
     * @param name   name
     * @param type   type
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> add(String name, Class<?> type, Function<P, ?> getter) {
        var tgType = TgDataType.of(type);
        return add(name, tgType, getter);
    }

    /**
     * add variable.
     *
     * @param <V>       value type
     * @param name      name
     * @param type      type
     * @param getter    getter from parameter
     * @param converter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> add(String name, Class<?> type, Function<P, V> getter, Function<V, ?> converter) {
        return add(name, type, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable.
     *
     * @param <V>      value type
     * @param variable variable
     * @param getter   getter from parameter
     * @return this
     */
    public <V> TgEntityParameterMapping<P> add(TgBindVariable<V> variable, Function<P, V> getter) {
        // return add(variable.name(), variable.type(), getter);
        addVariable(variable.name(), variable.type());
        parameterConverterList.add((parameter, convertUtil, closeableSet) -> {
            V value = getter.apply(parameter);
            return variable.bind(value).toLowParameter(closeableSet);
        });
        return this;
    }

    /**
     * add variable.
     *
     * @param name name
     * @param type type
     */
    protected void addVariable(String name, TgDataType type) {
        var lowPlaceholder = Placeholders.of(name, type.getLowDataType());
        lowPlaceholderList.add(lowPlaceholder);
    }

    @Override
    public List<Placeholder> toLowPlaceholderList() {
        return lowPlaceholderList;
    }

    @Override
    public List<Parameter> toLowParameterList(P parameter, IceaxeConvertUtil convertUtil, IceaxeCloseableSet closeableSet) throws IOException {
        var list = new ArrayList<Parameter>(parameterConverterList.size());
        for (var converter : parameterConverterList) {
            list.add(converter.apply(parameter, convertUtil, closeableSet));
        }
        return list;
    }
}
