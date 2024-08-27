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
package com.tsurugidb.iceaxe.util.function;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * {@link Supplier} with IOException.
 *
 * @param <T> the type of results supplied by this supplier
 */
@FunctionalInterface
public interface IoSupplier<T> {
    /**
     * Gets a result.
     *
     * @return a result
     * @throws IOException          if an I/O error occurs while execute
     * @throws InterruptedException if interrupted while execute
     */
    T get() throws IOException, InterruptedException;
}
