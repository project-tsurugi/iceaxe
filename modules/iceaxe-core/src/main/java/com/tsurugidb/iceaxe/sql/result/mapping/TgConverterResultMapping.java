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
package com.tsurugidb.iceaxe.sql.result.mapping;

import java.io.IOException;

import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.function.TsurugiTransactionFunction;

/**
 * Tsurugi Result Mapping.
 *
 * @param <R> result type
 */
public class TgConverterResultMapping<R> extends TgResultMapping<R> {

    /**
     * create Result Mapping.
     *
     * @param <R>             result type
     * @param resultConverter converter from TsurugiResultRecord to R
     * @return Result Mapping
     */
    public static <R> TgConverterResultMapping<R> of(TsurugiTransactionFunction<TsurugiResultRecord, R> resultConverter) {
        return new TgConverterResultMapping<>(resultConverter);
    }

    private final TsurugiTransactionFunction<TsurugiResultRecord, R> resultConverter;

    /**
     * Tsurugi Result Mapping.
     *
     * @param resultConverter converter from TsurugiResultRecord to R
     */
    public TgConverterResultMapping(TsurugiTransactionFunction<TsurugiResultRecord, R> resultConverter) {
        this.resultConverter = resultConverter;
    }

    @Override
    protected R convert(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException {
        return resultConverter.apply(record);
    }
}
