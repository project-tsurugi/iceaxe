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
package com.tsurugidb.iceaxe.util;

/**
 * AutoCloseable with timeout.
 *
 * @since 1.4.0
 */
@FunctionalInterface
@IceaxeInternal
public interface IceaxeTimeoutCloseable extends AutoCloseable {

    @Override
    public default void close() throws Exception {
        throw new AssertionError("do override");
    }

    /**
     * Closes this resource.
     *
     * @param timeoutNanos close timeout
     * @throws Exception if this resource cannot be closed
     */
    public void close(long timeoutNanos) throws Exception;
}
