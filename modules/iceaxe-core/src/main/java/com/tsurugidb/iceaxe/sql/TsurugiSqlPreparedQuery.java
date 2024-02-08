package com.tsurugidb.iceaxe.sql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.event.TsurugiSqlPreparedQueryEventListener;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi SQL prepared query (select).
 *
 * @param <P> parameter type
 * @param <R> result type
 */
public class TsurugiSqlPreparedQuery<P, R> extends TsurugiSqlPrepared<P> {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiSqlPreparedQuery.class);

    private final TgResultMapping<R> resultMapping;
    private List<TsurugiSqlPreparedQueryEventListener<P, R>> eventListenerList = null;

    /**
     * Creates a new instance.
     * <p>
     * Call {@link #initialize(FutureResponse)} after construct.
     * </p>
     *
     * @param session          session
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @param resultMapping    result mapping
     */
    @IceaxeInternal
    public TsurugiSqlPreparedQuery(TsurugiSession session, String sql, TgParameterMapping<P> parameterMapping, TgResultMapping<R> resultMapping) {
        super(session, sql, parameterMapping);
        this.resultMapping = resultMapping;
    }

    /**
     * add event listener.
     *
     * @param listener event listener
     * @return this
     */
    public TsurugiSql addEventListener(TsurugiSqlPreparedQueryEventListener<P, R> listener) {
        if (this.eventListenerList == null) {
            this.eventListenerList = new ArrayList<>();
        }
        eventListenerList.add(listener);
        return this;
    }

    private void event(Throwable occurred, Consumer<TsurugiSqlPreparedQueryEventListener<P, R>> action) {
        if (this.eventListenerList != null) {
            try {
                for (var listener : eventListenerList) {
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
     * @param parameter   SQL parameter
     * @return SQL result
     * @throws IOException                 if an I/O error occurs while execute query
     * @throws InterruptedException        if interrupted while execute query
     * @throws TsurugiTransactionException if server error occurs while execute query
     * @see TsurugiTransaction#executeQuery(TsurugiSqlPreparedQuery, Object)
     */
    public TsurugiQueryResult<R> execute(TsurugiTransaction transaction, P parameter) throws IOException, InterruptedException, TsurugiTransactionException {
        checkClose();

        LOG.trace("execute start");
        int sqlExecuteId = getNewIceaxeSqlExecuteId();
        event(null, listener -> listener.executeQueryStart(transaction, this, parameter, sqlExecuteId));

        TsurugiQueryResult<R> result;
        try {
            var lowPs = getLowPreparedStatement();
            var lowParameterList = getLowParameterList(parameter);
            var lowResultSetFuture = transaction.executeLow(lowTransaction -> lowTransaction.executeQuery(lowPs, lowParameterList));
            LOG.trace("execute started");

            var convertUtil = getConvertUtil(resultMapping.getConvertUtil());
            result = new TsurugiQueryResult<>(sqlExecuteId, transaction, this, parameter, lowResultSetFuture, resultMapping, convertUtil);
        } catch (Throwable e) {
            event(e, listener -> listener.executeQueryStartException(transaction, this, parameter, sqlExecuteId, e));
            throw e;
        }

        event(null, listener -> listener.executeQueryStarted(transaction, this, parameter, result));
        return result;
    }
}
