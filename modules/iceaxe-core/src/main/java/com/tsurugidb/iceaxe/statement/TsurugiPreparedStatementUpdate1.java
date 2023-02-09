package com.tsurugidb.iceaxe.statement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.result.TsurugiResultCount;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.event.TsurugiSqlPreparedStatementEventListener;
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
//TODO rename to TsurugiSqlPreparedStatement
public class TsurugiPreparedStatementUpdate1<P> extends TsurugiSqlPrepared<P> {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiPreparedStatementUpdate1.class);

    private List<TsurugiSqlPreparedStatementEventListener<P>> eventListenerList = null;

    // internal
    public TsurugiPreparedStatementUpdate1(TsurugiSession session, String sql, FutureResponse<PreparedStatement> lowPreparedStatementFuture, TgParameterMapping<P> parameterMapping)
            throws IOException {
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

    protected final void event(Consumer<TsurugiSqlPreparedStatementEventListener<P>> action) {
        if (this.eventListenerList != null) {
            for (var listener : eventListenerList) {
                action.accept(listener);
            }
        }
    }

    /**
     * execute statement
     *
     * @param transaction Transaction
     * @param parameter   SQL parameter
     * @return result
     * @throws IOException
     * @throws TsurugiTransactionException
     * @see TsurugiTransaction#executeStatement(TsurugiPreparedStatementUpdate1, P)
     */
    public TsurugiResultCount execute(TsurugiTransaction transaction, P parameter) throws IOException, TsurugiTransactionException {
        checkClose();

        var lowPs = getLowPreparedStatement();
        var lowParameterList = getLowParameterList(parameter);
        LOG.trace("executeStatement start");
        event(listener -> listener.executeStatementStart(transaction, this, parameter));
        var lowResultFuture = transaction.executeLow(lowTransaction -> lowTransaction.executeStatement(lowPs, lowParameterList));
        LOG.trace("executeStatement started");
        var result = new TsurugiResultCount(transaction, lowResultFuture);
        event(listener -> listener.executeStatementStarted(transaction, this, parameter, result));
        return result;
    }

    /**
     * execute statement
     *
     * @param transaction Transaction
     * @param parameter   SQL parameter
     * @return row count
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Deprecated(forRemoval = true)
    public int executeAndGetCount(TsurugiTransaction transaction, P parameter) throws IOException, TsurugiTransactionException {
        return transaction.executeAndGetCount(this, parameter);
    }

    /**
     * execute statement
     *
     * @param tm        Transaction Manager
     * @param parameter SQL parameter
     * @return row count
     * @throws IOException
     */
    @Deprecated(forRemoval = true)
    public int executeAndGetCount(TsurugiTransactionManager tm, P parameter) throws IOException {
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
     */
    @Deprecated(forRemoval = true)
    public int executeAndGetCount(TsurugiTransactionManager tm, TgTmSetting setting, P parameter) throws IOException {
        return tm.executeAndGetCount(setting, this, parameter);
    }
}
