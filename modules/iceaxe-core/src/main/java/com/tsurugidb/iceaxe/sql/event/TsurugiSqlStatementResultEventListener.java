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

import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.iceaxe.sql.TsurugiSqlStatement;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.sql.result.event.TsurugiStatementResultEventListener;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiSqlStatement} with {@link TsurugiStatementResult} event listener.
 */
public interface TsurugiSqlStatementResultEventListener extends TsurugiSqlStatementEventListener {

    @Override
    @OverridingMethodsMustInvokeSuper
    default void executeStatementStarted(TsurugiTransaction transaction, TsurugiSqlStatement ps, TsurugiStatementResult result) {
        result.addEventListener(new TsurugiStatementResultEventListener() {
            @Override
            public void endResult(TsurugiStatementResult result, @Nullable Throwable occurred) {
                executeStatementEnd(transaction, ps, result, occurred);
            }

            @Override
            public void closeResult(TsurugiStatementResult result, long timeoutNanos, @Nullable Throwable occurred) {
                executeStatementClose(transaction, ps, result, timeoutNanos, occurred);
            }
        });

        executeStatementStarted2(transaction, ps, result);
    }

    /**
     * called when execute statement started.
     *
     * @param transaction transaction
     * @param ps          SQL definition
     * @param result      SQL result
     */
    default void executeStatementStarted2(TsurugiTransaction transaction, TsurugiSqlStatement ps, TsurugiStatementResult result) {
        // do override
    }

    /**
     * called when execute statement end.
     *
     * @param transaction transaction
     * @param ps          SQL definition
     * @param result      SQL result
     * @param occurred    exception
     */
    default void executeStatementEnd(TsurugiTransaction transaction, TsurugiSqlStatement ps, TsurugiStatementResult result, @Nullable Throwable occurred) {
        // do override
    }

    /**
     * called when close result.
     *
     * @param transaction  transaction
     * @param ps           SQL definition
     * @param result       SQL result
     * @param timeoutNanos close timeout
     * @param occurred     exception
     */
    default void executeStatementClose(TsurugiTransaction transaction, TsurugiSqlStatement ps, TsurugiStatementResult result, long timeoutNanos, @Nullable Throwable occurred) {
        // do override
    }
}
