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

import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.sql.proto.SqlRequest.Placeholder;;

/**
 * Tsurugi Parameter Mapping for Empty.
 *
 * @param <P> parameter type
 */
public class TgEmptyParameterMapping<P> extends TgParameterMapping<P> {

    private static final TgEmptyParameterMapping<?> INSTANCE = new TgEmptyParameterMapping<>();

    /**
     * create Parameter Mapping.
     *
     * @param <P> parameter type
     * @return Tsurugi Parameter Mapping
     */
    public static <P> TgEmptyParameterMapping<P> of() {
        @SuppressWarnings("unchecked")
        var r = (TgEmptyParameterMapping<P>) INSTANCE;
        return r;
    }

    @Override
    public List<Placeholder> toLowPlaceholderList() {
        return List.of();
    }

    @Override
    public List<Parameter> toLowParameterList(P parameter, IceaxeConvertUtil convertUtil) {
        return List.of();
    }
}
