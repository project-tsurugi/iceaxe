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
package com.tsurugidb.iceaxe.session.event;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.session.TgSessionShutdownType;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlStatement;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;

/**
 * {@link TsurugiSession} event listener.
 */
public interface TsurugiSessionEventListener {

    /**
     * called when create query.
     *
     * @param <R> result type
     * @param ps  query
     */
    default <R> void createQuery(TsurugiSqlQuery<R> ps) {
        // do override
    }

    /**
     * called when create prepared query.
     *
     * @param <P> parameter type
     * @param <R> result type
     * @param ps  query
     */
    default <P, R> void createQuery(TsurugiSqlPreparedQuery<P, R> ps) {
        // do override
    }

    /**
     * called when create statement.
     *
     * @param ps statement
     */
    default void createStatement(TsurugiSqlStatement ps) {
        // do override
    }

    /**
     * called when create prepared statement.
     *
     * @param <P> parameter type
     * @param ps  statement
     */
    default <P> void createStatement(TsurugiSqlPreparedStatement<P> ps) {
        // do override
    }

    /**
     * called when create transaction manager.
     *
     * @param tm transaction manager
     */
    default void createTransactionManager(TsurugiTransactionManager tm) {
        // do override
    }

    /**
     * called when create transaction.
     *
     * @param transaction transaction
     */
    default void createTransaction(TsurugiTransaction transaction) {
        // do override
    }

    /**
     * called when shutdown session.
     *
     * @param session      session
     * @param shutdownType shutdown type
     * @param timeoutNanos shutdown timeout
     * @param occurred     exception
     * @since 1.4.0
     */
    default void shutdownSession(TsurugiSession session, TgSessionShutdownType shutdownType, long timeoutNanos, @Nullable Throwable occurred) {
        // do override
    }

    /**
     * called when close session.
     *
     * @param session      session
     * @param timeoutNanos close timeout
     * @param occurred     exception
     */
    default void closeSession(TsurugiSession session, long timeoutNanos, @Nullable Throwable occurred) {
        // do override
    }
}
