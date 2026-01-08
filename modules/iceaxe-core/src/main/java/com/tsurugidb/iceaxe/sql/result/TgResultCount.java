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
package com.tsurugidb.iceaxe.sql.result;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.tsubakuro.sql.CounterType;
import com.tsurugidb.tsubakuro.sql.ExecuteResult;

/**
 * Tsurugi SQL Result count.
 *
 * @see TsurugiStatementResult
 * @since 1.1.0
 */
@ThreadSafe
public class TgResultCount {

    private final ExecuteResult lowExecuteResult;

    /**
     * Creates a new instance.
     *
     * @param lowExecuteResult execute result
     */
    @IceaxeInternal
    public TgResultCount(@Nonnull ExecuteResult lowExecuteResult) {
        this.lowExecuteResult = Objects.requireNonNull(lowExecuteResult);
    }

    /**
     * get counter map.
     *
     * @return counter map
     */
    public Map<CounterType, Long> getLowCounterMap() {
        var counterMap = lowExecuteResult.getCounters();
        return (counterMap != null) ? counterMap : Map.of();
    }

    /**
     * get count.
     *
     * @return the row count for SQL Data Manipulation Language (DML) statements
     */
    public long getTotalCount() {
        var counterMap = getLowCounterMap();
        long total = 0;
        for (var count : counterMap.values()) {
            total += (count != null) ? count : 0L;
        }
        return total;
    }

    /**
     * get inserted count.
     *
     * @return inserted count
     */
    public long getInsertedCount() {
        return getCount(CounterType.INSERTED_ROWS);
    }

    /**
     * get updated count.
     *
     * @return updated count
     */
    public long getUpdatedCount() {
        return getCount(CounterType.UPDATED_ROWS);
    }

    /**
     * get merged count.
     *
     * @return merged count
     */
    public long getMergedCount() {
        return getCount(CounterType.MERGED_ROWS);
    }

    /**
     * get deleted count.
     *
     * @return deleted count
     */
    public long getDeletedCount() {
        return getCount(CounterType.DELETED_ROWS);
    }

    /**
     * get count.
     *
     * @param lowCounterType counter type
     * @return row count
     */
    public long getCount(CounterType lowCounterType) {
        var counterMap = getLowCounterMap();
        return counterMap.getOrDefault(lowCounterType, 0L);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + getLowCounterMap();
    }
}
