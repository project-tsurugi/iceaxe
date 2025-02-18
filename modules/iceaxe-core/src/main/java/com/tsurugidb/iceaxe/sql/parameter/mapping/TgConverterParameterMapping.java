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

import java.util.List;
import java.util.function.Function;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.sql.proto.SqlRequest.Placeholder;;

/**
 * Tsurugi Parameter Mapping for Entity.
 *
 * @param <P> parameter type (e.g. Entity)
 */
public class TgConverterParameterMapping<P> extends TgParameterMapping<P> {

    /**
     * create Parameter Mapping.
     *
     * @param <P>                parameter type
     * @param variables          bind variables
     * @param parameterConverter converter from P to Parameter
     * @return Tsurugi Parameter Mapping
     */
    public static <P> TgConverterParameterMapping<P> of(TgBindVariables variables, Function<P, TgBindParameters> parameterConverter) {
        return new TgConverterParameterMapping<>(variables, parameterConverter);
    }

    private final TgBindVariables variables;
    private final Function<P, TgBindParameters> parameterConverter;

    /**
     * Creates a new instance.
     *
     * @param variables          bind variables
     * @param parameterConverter converter from P to Parameter
     */
    public TgConverterParameterMapping(TgBindVariables variables, Function<P, TgBindParameters> parameterConverter) {
        this.variables = variables;
        this.parameterConverter = parameterConverter;
    }

    @Override
    public List<Placeholder> toLowPlaceholderList() {
        return variables.toLowPlaceholderList();
    }

    @Override
    public List<Parameter> toLowParameterList(P parameter, IceaxeConvertUtil convertUtil) {
        return parameterConverter.apply(parameter).toLowParameterList();
    }
}
