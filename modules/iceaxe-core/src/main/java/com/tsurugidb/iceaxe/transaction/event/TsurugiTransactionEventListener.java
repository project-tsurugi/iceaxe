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
package com.tsurugidb.iceaxe.transaction.event;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.sql.TsurugiSql;
import com.tsurugidb.iceaxe.sql.result.TsurugiSqlResult;
import com.tsurugidb.iceaxe.transaction.TgCommitOption;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction.TgTxMethod;

/**
 * {@link TsurugiTransaction} event listener.
 */
public interface TsurugiTransactionEventListener {

    /**
     * called when low transaction get start.
     *
     * @param transaction transaction
     */
    default void lowTransactionGetStart(TsurugiTransaction transaction) {
        // do override
    }

    /**
     * called when low transaction get end.
     *
     * @param transaction   transaction
     * @param transactionId transactionId
     * @param occurred      exception
     */
    default void lowTransactionGetEnd(TsurugiTransaction transaction, @Nullable String transactionId, @Nullable Throwable occurred) {
        // do override
    }

    /**
     * called when execute start.
     *
     * @param transaction       transaction
     * @param method            execute method
     * @param iceaxeTxExecuteId iceaxe tx executeId
     * @param ps                SQL definition
     * @param parameter         SQL parameter
     */
    default void executeStart(TsurugiTransaction transaction, TgTxMethod method, int iceaxeTxExecuteId, TsurugiSql ps, @Nullable Object parameter) {
        // do override
    }

    /**
     * called when execute end.
     *
     * @param transaction       transaction
     * @param method            execute method
     * @param iceaxeTxExecuteId iceaxe tx executeId
     * @param ps                SQL definition
     * @param parameter         SQL parameter
     * @param result            SQL result
     * @param occurred          exception
     */
    default void executeEnd(TsurugiTransaction transaction, TgTxMethod method, int iceaxeTxExecuteId, TsurugiSql ps, @Nullable Object parameter, @Nullable TsurugiSqlResult result,
            @Nullable Throwable occurred) {
        // do override
    }

    /**
     * called when commit start.
     *
     * @param transaction transaction
     * @param commitType  commit type
     * @see #commitStart(TsurugiTransaction, TgCommitOption)
     */
    @Deprecated(since = "X.X.X", forRemoval = true)
    default void commitStart(TsurugiTransaction transaction, TgCommitType commitType) {
        // do override
    }

    /**
     * called when commit start.
     *
     * @param transaction  transaction
     * @param commitOption commit option
     * @since X.X.X
     */
    default void commitStart(TsurugiTransaction transaction, TgCommitOption commitOption) {
        // do override
        commitStart(transaction, commitOption.commitType());
    }

    /**
     * called when commit end.
     *
     * @param transaction transaction
     * @param commitType  commit type
     * @param occurred    exception
     * @see #commitEnd(TsurugiTransaction, TgCommitOption, Throwable)
     */
    @Deprecated(since = "X.X.X", forRemoval = true)
    default void commitEnd(TsurugiTransaction transaction, TgCommitType commitType, @Nullable Throwable occurred) {
        // do override
    }

    /**
     * called when commit end.
     *
     * @param transaction  transaction
     * @param commitOption commit type
     * @param occurred     exception
     * @since X.X.X
     */
    default void commitEnd(TsurugiTransaction transaction, TgCommitOption commitOption, @Nullable Throwable occurred) {
        // do override
        commitEnd(transaction, commitOption.commitType(), occurred);
    }

    /**
     * called when rollback start.
     *
     * @param transaction transaction
     */
    default void rollbackStart(TsurugiTransaction transaction) {
        // do override
    }

    /**
     * called when rollback end.
     *
     * @param transaction transaction
     * @param occurred    exception
     */
    default void rollbackEnd(TsurugiTransaction transaction, @Nullable Throwable occurred) {
        // do override
    }

    /**
     * called when close transaction.
     *
     * @param transaction  transaction
     * @param timeoutNanos close timeout
     * @param occurred     exception
     */
    default void closeTransaction(TsurugiTransaction transaction, long timeoutNanos, @Nullable Throwable occurred) {
        // do override
    }
}
