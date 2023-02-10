package com.tsurugidb.iceaxe.statement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultSet;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.event.TsurugiSqlPreparedQueryEventListener;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi SQL prepared query (select)
 *
 * @param <P> parameter type
 * @param <R> result type
 */
//TODO rename to TsurugiSqlPreparedQuery
public class TsurugiPreparedStatementQuery1<P, R> extends TsurugiSqlPrepared<P> {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiPreparedStatementQuery1.class);

    private final TgResultMapping<R> resultMapping;
    private List<TsurugiSqlPreparedQueryEventListener<P, R>> eventListenerList = null;

    // internal
    public TsurugiPreparedStatementQuery1(TsurugiSession session, String sql, FutureResponse<PreparedStatement> lowPreparedStatementFuture, TgParameterMapping<P> parameterMapping,
            TgResultMapping<R> resultMapping) throws IOException {
        super(session, sql, lowPreparedStatementFuture, parameterMapping);
        this.resultMapping = resultMapping;
    }

    /**
     * add event listener
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
     * execute query
     *
     * @param transaction Transaction
     * @param parameter   SQL parameter
     * @return Result Set
     * @throws IOException
     * @throws TsurugiTransactionException
     * @see TsurugiTransaction#executeQuery(TsurugiPreparedStatementQuery1, P)
     */
    public TsurugiResultSet<R> execute(TsurugiTransaction transaction, P parameter) throws IOException, TsurugiTransactionException {
        checkClose();

        LOG.trace("executeQuery start");
        int sqlExecuteId = getNewIceaxeSqlExecuteId();
        event(null, listener -> listener.executeQueryStart(transaction, this, parameter, sqlExecuteId));

        TsurugiResultSet<R> rs;
        try {
            var lowPs = getLowPreparedStatement();
            var lowParameterList = getLowParameterList(parameter);
            var lowResultSetFuture = transaction.executeLow(lowTransaction -> lowTransaction.executeQuery(lowPs, lowParameterList));
            LOG.trace("executeQuery started");

            var convertUtil = getConvertUtil(resultMapping.getConvertUtil());
            rs = new TsurugiResultSet<>(sqlExecuteId, transaction, lowResultSetFuture, resultMapping, convertUtil);
        } catch (Throwable e) {
            event(e, listener -> listener.executeQueryStartException(transaction, this, parameter, sqlExecuteId, e));
            throw e;
        }

        event(null, listener -> listener.executeQueryStarted(transaction, this, parameter, rs));
        return rs;
    }

    /**
     * execute query
     *
     * @param transaction Transaction
     * @param parameter   SQL parameter
     * @return record
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Deprecated(forRemoval = true)
    public Optional<R> executeAndFindRecord(TsurugiTransaction transaction, P parameter) throws IOException, TsurugiTransactionException {
        return transaction.executeAndFindRecord(this, parameter);
    }

    /**
     * execute query
     *
     * @param tm        Transaction Manager
     * @param parameter SQL parameter
     * @return record
     * @throws IOException
     */
    @Deprecated(forRemoval = true)
    public Optional<R> executeAndFindRecord(TsurugiTransactionManager tm, P parameter) throws IOException {
        return tm.executeAndFindRecord(this, parameter);
    }

    /**
     * execute query
     *
     * @param tm        Transaction Manager
     * @param setting   transaction manager settings
     * @param parameter SQL parameter
     * @return record
     * @throws IOException
     */
    @Deprecated(forRemoval = true)
    public Optional<R> executeAndFindRecord(TsurugiTransactionManager tm, TgTmSetting setting, P parameter) throws IOException {
        return tm.executeAndFindRecord(setting, this, parameter);
    }

    /**
     * execute query
     *
     * @param transaction Transaction
     * @param parameter   SQL parameter
     * @return list of record
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Deprecated(forRemoval = true)
    public List<R> executeAndGetList(TsurugiTransaction transaction, P parameter) throws IOException, TsurugiTransactionException {
        return transaction.executeAndGetList(this, parameter);
    }

    /**
     * execute query
     *
     * @param tm        Transaction Manager
     * @param parameter SQL parameter
     * @return list of record
     * @throws IOException
     */
    @Deprecated(forRemoval = true)
    public List<R> executeAndGetList(TsurugiTransactionManager tm, P parameter) throws IOException {
        return tm.executeAndGetList(this, parameter);
    }

    /**
     * execute query
     *
     * @param tm        Transaction Manager
     * @param setting   transaction manager settings
     * @param parameter SQL parameter
     * @return list of record
     * @throws IOException
     */
    @Deprecated(forRemoval = true)
    public List<R> executeAndGetList(TsurugiTransactionManager tm, TgTmSetting setting, P parameter) throws IOException {
        return tm.executeAndGetList(setting, this, parameter);
    }
}
