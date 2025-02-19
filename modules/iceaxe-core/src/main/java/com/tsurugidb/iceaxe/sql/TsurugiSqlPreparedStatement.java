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
package com.tsurugidb.iceaxe.sql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.event.TsurugiSqlPreparedStatementEventListener;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeCloseableSet;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi SQL prepared statement (insert/update/delete).
 *
 * @param <P> parameter type
 */
public class TsurugiSqlPreparedStatement<P> extends TsurugiSqlPrepared<P> {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiSqlPreparedStatement.class);

    private List<TsurugiSqlPreparedStatementEventListener<P>> eventListenerList = null;

    /**
     * Creates a new instance.
     * <p>
     * Call {@link #initialize(FutureResponse)} after construct.
     * </p>
     *
     * @param session          session
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     */
    @IceaxeInternal
    public TsurugiSqlPreparedStatement(TsurugiSession session, String sql, TgParameterMapping<P> parameterMapping) {
        super(session, sql, parameterMapping);
    }

    /**
     * add event listener.
     *
     * @param listener event listener
     * @return this
     */
    public TsurugiSql addEventListener(TsurugiSqlPreparedStatementEventListener<P> listener) {
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
    public Optional<TsurugiSqlPreparedStatementEventListener<P>> findEventListener(Predicate<TsurugiSqlPreparedStatementEventListener<P>> predicate) {
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

    private void event(Throwable occurred, Consumer<TsurugiSqlPreparedStatementEventListener<P>> action) {
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
     * execute statement.
     *
     * @param transaction Transaction
     * @param parameter   SQL parameter
     * @return SQL result
     * @throws IOException                 if an I/O error occurs while execute statement
     * @throws InterruptedException        if interrupted while execute statement
     * @throws TsurugiTransactionException if server error occurs while execute statement
     * @see TsurugiTransaction#executeStatement(TsurugiSqlPreparedStatement, Object)
     */
    public TsurugiStatementResult execute(TsurugiTransaction transaction, P parameter) throws IOException, InterruptedException, TsurugiTransactionException {
        checkClose();

        LOG.trace("execute start");
        int sqlExecuteId = getNewIceaxeSqlExecuteId();
        event(null, listener -> listener.executeStatementStart(transaction, this, parameter, sqlExecuteId));

        TsurugiStatementResult result;
        try {
            var lowPs = getLowPreparedStatement();
            var closeableSet = new IceaxeCloseableSet();
            var lowParameterList = getLowParameterList(parameter, closeableSet);
            var lowResultFuture = transaction.executeLow(lowTransaction -> lowTransaction.executeStatement(lowPs, lowParameterList));
            LOG.trace("execute started");

            result = new TsurugiStatementResult(sqlExecuteId, transaction, this, parameter, closeableSet);
            result.initialize(lowResultFuture);
        } catch (Throwable e) {
            event(e, listener -> listener.executeStatementStartException(transaction, this, parameter, sqlExecuteId, e));
            throw e;
        }

        event(null, listener -> listener.executeStatementStarted(transaction, this, parameter, result));
        return result;
    }

    /**
     * <em>This method is not yet implemented:</em> execute batch.
     *
     * @param transaction   Transaction
     * @param parameterList SQL parameter
     * @return SQL result
     * @throws IOException                 if an I/O error occurs while execute batch
     * @throws InterruptedException        if interrupted while execute batch
     * @throws TsurugiTransactionException if server error occurs while execute batch
     * @see TsurugiTransaction#executeBatch(TsurugiSqlPreparedStatement, Collection)
     */
    public TsurugiStatementResult executeBatch(TsurugiTransaction transaction, Collection<P> parameterList) throws IOException, InterruptedException, TsurugiTransactionException {
        throw new UnsupportedOperationException("not yet implements"); // TODO executeBatch
//        checkClose();
//
//        LOG.trace("executeBatch start");
//        int sqlExecuteId = getNewIceaxeSqlExecuteId();
//        event(null, listener -> listener.executeBatchStart(transaction, this, parameterList, sqlExecuteId));
//
//        TsurugiStatementResult result;
//        try {
//            var lowPs = getLowPreparedStatement();
//            var lowParameterList = new ArrayList<List<Parameter>>(parameterList.size());
//            for (P parameter : parameterList) {
//                lowParameterList.add(getLowParameterList(parameter));
//            }
//            var lowResultFuture = transaction.executeLow(lowTransaction -> lowTransaction.batch(lowPs, lowParameterList));
//            LOG.trace("executeBatch started");
//
//            result = new TsurugiStatementResult(sqlExecuteId, transaction, this, parameterList);
//            result.initialize(lowResultFuture);
//        } catch (Throwable e) {
//            event(e, listener -> listener.executeBatchStartException(transaction, this, parameterList, sqlExecuteId, e));
//            throw e;
//        }
//
//        event(null, listener -> listener.executeBatchStarted(transaction, this, parameterList, result));
//        return result;
    }
}
