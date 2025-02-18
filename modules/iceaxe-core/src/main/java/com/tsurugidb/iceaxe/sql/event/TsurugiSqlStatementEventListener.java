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
package com.tsurugidb.iceaxe.sql.event;

import com.tsurugidb.iceaxe.sql.TsurugiSqlStatement;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiSqlStatement} event listener.
 */
public interface TsurugiSqlStatementEventListener {

    /**
     * called when execute statement start.
     *
     * @param transaction        transaction
     * @param ps                 SQL definition
     * @param iceaxeSqlExecuteId iceaxe SQL executeId
     */
    default void executeStatementStart(TsurugiTransaction transaction, TsurugiSqlStatement ps, int iceaxeSqlExecuteId) {
        // do override
    }

    /**
     * called when execute statement start error.
     *
     * @param transaction        transaction
     * @param ps                 SQL definition
     * @param iceaxeSqlExecuteId iceaxe SQL executeId
     * @param occurred           exception
     */
    default void executeStatementStartException(TsurugiTransaction transaction, TsurugiSqlStatement ps, int iceaxeSqlExecuteId, Throwable occurred) {
        // do override
    }

    /**
     * called when execute statement started.
     *
     * @param transaction transaction
     * @param ps          SQL definition
     * @param result      SQL result
     */
    default void executeStatementStarted(TsurugiTransaction transaction, TsurugiSqlStatement ps, TsurugiStatementResult result) {
        // do override
    }
}
