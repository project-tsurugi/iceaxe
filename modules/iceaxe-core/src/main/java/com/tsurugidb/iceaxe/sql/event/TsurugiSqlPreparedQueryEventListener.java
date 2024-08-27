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
package com.tsurugidb.iceaxe.sql.event;

import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiSqlPreparedQuery} event listener.
 *
 * @param <P> parameter type
 * @param <R> result type
 */
public interface TsurugiSqlPreparedQueryEventListener<P, R> {

    /**
     * called when execute query start.
     *
     * @param transaction        transaction
     * @param ps                 SQL definition
     * @param parameter          SQL parameter
     * @param iceaxeSqlExecuteId iceaxe SQL executeId
     */
    default void executeQueryStart(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<P, R> ps, P parameter, int iceaxeSqlExecuteId) {
        // do override
    }

    /**
     * called when execute query start error.
     *
     * @param transaction        transaction
     * @param ps                 SQL definition
     * @param parameter          SQL parameter
     * @param iceaxeSqlExecuteId iceaxe SQL executeId
     * @param occurred           exception
     */
    default void executeQueryStartException(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<P, R> ps, P parameter, int iceaxeSqlExecuteId, Throwable occurred) {
        // do override
    }

    /**
     * called when execute query started.
     *
     * @param transaction transaction
     * @param ps          SQL definition
     * @param parameter   SQL parameter
     * @param result      SQL result
     */
    default void executeQueryStarted(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<P, R> ps, P parameter, TsurugiQueryResult<R> result) {
        // do override
    }
}
