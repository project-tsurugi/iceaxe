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
package com.tsurugidb.iceaxe.sql.parameter;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.mapping.TgConverterParameterMapping;
import com.tsurugidb.iceaxe.sql.parameter.mapping.TgEmptyParameterMapping;
import com.tsurugidb.iceaxe.sql.parameter.mapping.TgEntityParameterMapping;
import com.tsurugidb.iceaxe.sql.parameter.mapping.TgSingleParameterMapping;
import com.tsurugidb.iceaxe.util.IceaxeCloseableSet;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.sql.proto.SqlRequest.Placeholder;

/**
 * Tsurugi Parameter Mapping.
 *
 * @param <P> parameter type
 */
public abstract class TgParameterMapping<P> {

    /**
     * create Parameter Mapping.
     *
     * @param <P>   parameter type
     * @param clazz parameter class
     * @return parameter mapping
     */
    public static <P> TgEntityParameterMapping<P> of(Class<P> clazz) {
        return TgEntityParameterMapping.of(clazz);
    }

    /**
     * create Parameter Mapping.
     *
     * @param variables bind variables
     * @return parameter mapping
     */
    public static TgParameterMapping<TgBindParameters> of(TgBindVariable<?>... variables) {
        if (variables.length == 0) {
            return TgEmptyParameterMapping.of();
        }
        return of(TgBindVariables.of(variables));
    }

    /**
     * create Parameter Mapping.
     *
     * @param variables bind variables
     * @return parameter mapping
     */
    public static TgParameterMapping<TgBindParameters> of(Collection<? extends TgBindVariable<?>> variables) {
        return of(TgBindVariables.of(variables));
    }

    /**
     * create Parameter Mapping.
     *
     * @param variables bind variables
     * @return parameter mapping
     */
    public static TgParameterMapping<TgBindParameters> of(TgBindVariables variables) {
        return of(variables, TgBindParameters.IDENTITY);
    }

    /**
     * create Parameter Mapping.
     *
     * @param <P>                parameter type
     * @param variables          bind variables
     * @param parameterConverter converter from P to Parameter
     * @return parameter mapping
     */
    public static <P> TgParameterMapping<P> of(TgBindVariables variables, Function<P, TgBindParameters> parameterConverter) {
        return new TgConverterParameterMapping<>(variables, parameterConverter);
    }

    /**
     * create Parameter Mapping (single variable).
     *
     * @param <P>   parameter type
     * @param name  bind variable name
     * @param clazz parameter type
     * @return parameter mapping
     * @see TgSingleParameterMapping
     */
    public static <P> TgParameterMapping<P> ofSingle(String name, Class<P> clazz) {
        return TgSingleParameterMapping.of(name, clazz);
    }

    /**
     * create Parameter Mapping (single variable).
     *
     * @param <P>  parameter type
     * @param name bind variable name
     * @param type parameter type
     * @return parameter mapping
     * @see TgSingleParameterMapping
     */
    public static <P> TgParameterMapping<P> ofSingle(String name, TgDataType type) {
        return TgSingleParameterMapping.of(name, type);
    }

    /**
     * create Parameter Mapping (single variable).
     *
     * @param <P>      parameter type
     * @param variable bind variable
     * @return parameter mapping
     * @see TgSingleParameterMapping
     */
    public static <P> TgParameterMapping<P> ofSingle(TgBindVariable<P> variable) {
        return TgSingleParameterMapping.of(variable);
    }

    private IceaxeConvertUtil convertUtil = null;

    /**
     * set convert type utility.
     *
     * @param convertUtil convert type utility
     * @return this
     */
    public TgParameterMapping<P> setConvertUtil(IceaxeConvertUtil convertUtil) {
        this.convertUtil = convertUtil;
        return this;
    }

    /**
     * get convert type utility.
     *
     * @return convert type utility
     */
    public IceaxeConvertUtil getConvertUtil() {
        return this.convertUtil;
    }

    /**
     * convert to {@link Placeholder} list.
     *
     * @return placeholder list
     */
    @IceaxeInternal
    public abstract List<Placeholder> toLowPlaceholderList();

    /**
     * convert to {@link Parameter} list.
     *
     * @param parameter    parameter
     * @param convertUtil  convert type utility
     * @param closeableSet Closeable set for execute finished
     * @return parameter list
     * @throws IOException if an I/O error occurs
     */
    @IceaxeInternal
    public abstract List<Parameter> toLowParameterList(P parameter, IceaxeConvertUtil convertUtil, IceaxeCloseableSet closeableSet) throws IOException;
}
