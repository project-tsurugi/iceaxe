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
package com.tsurugidb.iceaxe.sql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.event.TsurugiSqlQueryEventListener;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeInternal;

/**
 * Tsurugi SQL query (select).
 *
 * @param <R> result type
 */
public class TsurugiSqlQuery<R> extends TsurugiSqlDirect {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiSqlQuery.class);

    private final TgResultMapping<R> resultMapping;
    private List<TsurugiSqlQueryEventListener<R>> eventListenerList = null;

    /**
     * Creates a new instance.
     * <p>
     * Call {@link #initialize()} after construct.
     * </p>
     *
     * @param session       session
     * @param sql           SQL
     * @param resultMapping result mapping
     */
    @IceaxeInternal
    public TsurugiSqlQuery(TsurugiSession session, String sql, TgResultMapping<R> resultMapping) {
        super(session, sql);
        this.resultMapping = resultMapping;
    }

    /**
     * add event listener.
     *
     * @param listener event listener
     * @return this
     */
    public TsurugiSql addEventListener(TsurugiSqlQueryEventListener<R> listener) {
        if (this.eventListenerList == null) {
            this.eventListenerList = new ArrayList<>();
        }
        eventListenerList.add(listener);
        return this;
    }

    /**
     * find event listener.
     *
     * @param predicate predicate for event listener
     * @return event listener
     * @since 1.3.0
     */
    public Optional<TsurugiSqlQueryEventListener<R>> findEventListener(Predicate<TsurugiSqlQueryEventListener<R>> predicate) {
        var listenerList = this.eventListenerList;
        if (listenerList != null) {
            for (var listener : listenerList) {
                if (predicate.test(listener)) {
                    return Optional.of(listener);
                }
            }
        }
        return Optional.empty();
    }

    private void event(Throwable occurred, Consumer<TsurugiSqlQueryEventListener<R>> action) {
        var listenerList = this.eventListenerList;
        if (listenerList != null) {
            try {
                for (var listener : listenerList) {
                    action.accept(listener);
                }
            } catch (Throwable e) {
                if (occurred != null) {
                    e.addSuppressed(occurred);
                }
                throw e;
            }
        }
    }

    /**
     * execute query.
     *
     * @param transaction Transaction
     * @return SQL result
     * @throws IOException                 if an I/O error occurs while execute query
     * @throws InterruptedException        if interrupted while execute query
     * @throws TsurugiTransactionException if server error occurs while execute query
     * @see TsurugiTransaction#executeQuery(TsurugiSqlQuery)
     */
    public TsurugiQueryResult<R> execute(TsurugiTransaction transaction) throws IOException, InterruptedException, TsurugiTransactionException {
        checkClose();

        LOG.trace("execute start");
        int sqlExecuteId = getNewIceaxeSqlExecuteId();
        event(null, listener -> listener.executeQueryStart(transaction, this, sqlExecuteId));

        TsurugiQueryResult<R> result;
        try {
            var lowResultSetFuture = transaction.executeLow(lowTransaction -> lowTransaction.executeQuery(sql));
            LOG.trace("execute started");

            var convertUtil = getConvertUtil(resultMapping.getConvertUtil());
            result = new TsurugiQueryResult<>(sqlExecuteId, transaction, this, null, resultMapping, convertUtil);
            result.initialize(lowResultSetFuture);
        } catch (Throwable e) {
            event(e, listener -> listener.executeQueryStartException(transaction, this, sqlExecuteId, e));
            throw e;
        }

        event(null, listener -> listener.executeQueryStarted(transaction, this, result));
        return result;
    }
}
