package com.tsurugidb.iceaxe.sql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.event.TsurugiSqlPreparedStatementEventListener;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
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

    private void event(Throwable occurred, Consumer<TsurugiSqlPreparedStatementEventListener<P>> action) {
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
            var lowParameterList = getLowParameterList(parameter);
            var lowResultFuture = transaction.executeLow(lowTransaction -> lowTransaction.executeStatement(lowPs, lowParameterList));
            LOG.trace("execute started");

            result = new TsurugiStatementResult(sqlExecuteId, transaction, this, parameter);
            result.initialize(lowResultFuture);
        } catch (Throwable e) {
            event(e, listener -> listener.executeStatementStartException(transaction, this, parameter, sqlExecuteId, e));
            throw e;
        }

        event(null, listener -> listener.executeStatementStarted(transaction, this, parameter, result));
        return result;
    }

    /**
     * execute batch.
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
/*
        checkClose();

        LOG.trace("executeBatch start");
        int sqlExecuteId = getNewIceaxeSqlExecuteId();
        event(null, listener -> listener.executeBatchStart(transaction, this, parameterList, sqlExecuteId));

        TsurugiStatementResult result;
        try {
            var lowPs = getLowPreparedStatement();
            var lowParameterList = new ArrayList<List<Parameter>>(parameterList.size());
            for (P parameter : parameterList) {
                lowParameterList.add(getLowParameterList(parameter));
            }
            var lowResultFuture = transaction.executeLow(lowTransaction -> lowTransaction.batch(lowPs, lowParameterList));
            LOG.trace("executeBatch started");

            result = new TsurugiStatementResult(sqlExecuteId, transaction, this, parameterList);
            result.initialize(lowResultFuture);
        } catch (Throwable e) {
            event(e, listener -> listener.executeBatchStartException(transaction, this, parameterList, sqlExecuteId, e));
            throw e;
        }

        event(null, listener -> listener.executeBatchStarted(transaction, this, parameterList, result));
        return result;
*/
    }
}
