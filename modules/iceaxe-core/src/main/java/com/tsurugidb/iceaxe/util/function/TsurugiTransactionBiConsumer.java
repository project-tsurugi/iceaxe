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
import java.util.function.BiConsumer;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * {@link BiConsumer} with IOException, TsurugiTransactionException.
 *
 * @param <T> the type of the first argument to the operation
 * @param <U> the type of the second argument to the operation
 */
@FunctionalInterface
public interface TsurugiTransactionBiConsumer<T, U> {
    /**
     * Performs this operation on the given arguments.
     *
     * @param t the first input argument
     * @param u the second input argument
     * @throws IOException                 if an I/O error occurs while execute
     * @throws InterruptedException        if interrupted while execute
     * @throws TsurugiTransactionException if server error occurs while execute
     */
    void accept(T t, U u) throws IOException, InterruptedException, TsurugiTransactionException;
}
