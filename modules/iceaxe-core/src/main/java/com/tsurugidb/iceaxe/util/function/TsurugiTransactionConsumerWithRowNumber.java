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
package com.tsurugidb.iceaxe.util.function;

import java.io.IOException;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * {@link TsurugiTransactionConsumer} with row number.
 *
 * @param <T> the type of the input to the operation
 * @since X.X.X
 */
@FunctionalInterface
public interface TsurugiTransactionConsumerWithRowNumber<T> {
    /**
     * Performs this operation on the given argument.
     *
     * @param rowNumber row number (0 origin)
     * @param t         the input argument
     * @throws IOException                 if an I/O error occurs while execute
     * @throws InterruptedException        if interrupted while execute
     * @throws TsurugiTransactionException if server error occurs while execute
     */
    void accept(int rowNumber, T t) throws IOException, InterruptedException, TsurugiTransactionException;
}
