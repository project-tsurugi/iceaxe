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
package com.tsurugidb.iceaxe.sql.result.event;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;

/**
 * {@link TsurugiStatementResult} event listener.
 */
public interface TsurugiStatementResultEventListener {

    /**
     * called when execute end.
     *
     * @param result   SQL result
     * @param occurred exception
     */
    default void endResult(TsurugiStatementResult result, @Nullable Throwable occurred) {
        // do override
    }

    /**
     * called when close result.
     *
     * @param result       SQL result
     * @param timeoutNanos close timeout
     * @param occurred     exception
     */
    default void closeResult(TsurugiStatementResult result, long timeoutNanos, @Nullable Throwable occurred) {
        // do override
    }
}
