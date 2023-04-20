package com.tsurugidb.iceaxe.sql;

import java.io.IOException;
import java.util.ArrayList;
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
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi SQL prepared statement (insert/update/delete)
 *
 * @param <P> parameter type
 */
public class TsurugiSqlPreparedStatement<P> extends TsurugiSqlPrepared<P> {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiSqlPreparedStatement.class);

    private List<TsurugiSqlPreparedStatementEventListener<P>> eventListenerList = null;

    // internal
    public TsurugiSqlPreparedStatement(TsurugiSession session, String sql, FutureResponse<PreparedStatement> lowPreparedStatementFuture, TgParameterMapping<P> parameterMapping) throws IOException {
        super(session, sql, lowPreparedStatementFuture, parameterMapping);
    }

    /**
     * add event listener
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
     * execute statement
     *
     * @param transaction Transaction
     * @param parameter   SQL parameter
     * @return SQL result
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @see TsurugiTransaction#executeStatement(TsurugiSqlPreparedStatement, P)
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

            result = new TsurugiStatementResult(sqlExecuteId, transaction, this, parameter, lowResultFuture);
        } catch (Throwable e) {
            event(e, listener -> listener.executeStatementStartException(transaction, this, parameter, sqlExecuteId, e));
            throw e;
        }

        event(null, listener -> listener.executeStatementStarted(transaction, this, parameter, result));
        return result;
    }

    /**
     * execute statement
     *
     * @param transaction Transaction
     * @param parameter   SQL parameter
     * @return row count
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    @Deprecated(forRemoval = true)
    public int executeAndGetCount(TsurugiTransaction transaction, P parameter) throws IOException, InterruptedException, TsurugiTransactionException {
        return transaction.executeAndGetCount(this, parameter);
    }

    /**
     * execute statement
     *
     * @param tm        Transaction Manager
     * @param parameter SQL parameter
     * @return row count
     * @throws IOException
     * @throws InterruptedException
     */
    @Deprecated(forRemoval = true)
    public int executeAndGetCount(TsurugiTransactionManager tm, P parameter) throws IOException, InterruptedException {
        return tm.executeAndGetCount(this, parameter);
    }

    /**
     * execute statement
     *
     * @param tm        Transaction Manager
     * @param setting   transaction manager setting
     * @param parameter SQL parameter
     * @return row count
     * @throws IOException
     * @throws InterruptedException
     */
    @Deprecated(forRemoval = true)
    public int executeAndGetCount(TsurugiTransactionManager tm, TgTmSetting setting, P parameter) throws IOException, InterruptedException {
        return tm.executeAndGetCount(setting, this, parameter);
    }
}
