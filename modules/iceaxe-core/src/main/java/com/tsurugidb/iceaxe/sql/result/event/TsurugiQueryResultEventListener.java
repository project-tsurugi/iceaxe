/*
 * Copyright 2023-2026 Project Tsurugi.
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

import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;

/**
 * {@link TsurugiQueryResult} event listener.
 *
 * @param <R> record type
 */
public interface TsurugiQueryResultEventListener<R> {

    /**
     * called when read a record.
     *
     * @param result SQL result
     * @param record record
     */
    default void readRecord(TsurugiQueryResult<R> result, R record) {
        // do override
    }

    /**
     * called when occurs exception.
     *
     * @param result   SQL result
     * @param occurred exception
     */
    default void readException(TsurugiQueryResult<R> result, Throwable occurred) {
        // do override
    }

    /**
     * called when execute end.
     *
     * @param result SQL result
     */
    default void endResult(TsurugiQueryResult<R> result) {
        // do override
    }

    /**
     * called when close result.
     *
     * @param result       SQL result
     * @param timeoutNanos close timeout
     * @param occurred     exception
     */
    default void closeResult(TsurugiQueryResult<R> result, long timeoutNanos, @Nullable Throwable occurred) {
        // do override
    }
}
