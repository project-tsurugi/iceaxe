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
import com.tsurugidb.iceaxe.statement.event.TsurugiSqlQueryEventListener;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;

/**
 * Tsurugi SQL query (select)
 *
 * @param <R> result type
 */
// TODO rename to TsurugiSqlQuery
public class TsurugiPreparedStatementQuery0<R> extends TsurugiSqlDirect {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiPreparedStatementQuery0.class);

    private final TgResultMapping<R> resultMapping;
    private List<TsurugiSqlQueryEventListener<R>> eventListenerList = null;

    // internal
    public TsurugiPreparedStatementQuery0(TsurugiSession session, String sql, TgResultMapping<R> resultMapping) throws IOException {
        super(session, sql);
        this.resultMapping = resultMapping;
    }

    /**
     * add event listener
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

    protected final void event(Consumer<TsurugiSqlQueryEventListener<R>> action) {
        if (this.eventListenerList != null) {
            for (var listener : eventListenerList) {
                action.accept(listener);
            }
        }
    }

    /**
     * execute query
     *
     * @param transaction Transaction
     * @return Result Set
     * @throws IOException
     * @throws TsurugiTransactionException
     * @see TsurugiTransaction#executeQuery(TsurugiPreparedStatementQuery0)
     */
    public TsurugiResultSet<R> execute(TsurugiTransaction transaction) throws IOException, TsurugiTransactionException {
        checkClose();

        LOG.trace("executeQuery start");
        event(listener -> listener.executeQueryStart(transaction, this));
        var lowResultSetFuture = transaction.executeLow(lowTransaction -> lowTransaction.executeQuery(sql));
        LOG.trace("executeQuery started");
        var convertUtil = getConvertUtil(resultMapping.getConvertUtil());
        var result = new TsurugiResultSet<>(transaction, lowResultSetFuture, resultMapping, convertUtil);
        event(listener -> listener.executeQueryStarted(transaction, this, result));
        return result;
    }

    /**
     * execute query
     *
     * @param transaction Transaction
     * @return record
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Deprecated(forRemoval = true)
    public Optional<R> executeAndFindRecord(TsurugiTransaction transaction) throws IOException, TsurugiTransactionException {
        return transaction.executeAndFindRecord(this);
    }

    /**
     * execute query
     *
     * @param tm Transaction Manager
     * @return record
     * @throws IOException
     */
    @Deprecated(forRemoval = true)
    public Optional<R> executeAndFindRecord(TsurugiTransactionManager tm) throws IOException {
        return tm.executeAndFindRecord(this);
    }

    /**
     * execute query
     *
     * @param tm      Transaction Manager
     * @param setting transaction manager settings
     * @return record
     * @throws IOException
     */
    @Deprecated(forRemoval = true)
    public Optional<R> executeAndFindRecord(TsurugiTransactionManager tm, TgTmSetting setting) throws IOException {
        return tm.executeAndFindRecord(setting, this);
    }

    /**
     * execute query
     *
     * @param transaction Transaction
     * @return list of record
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Deprecated(forRemoval = true)
    public List<R> executeAndGetList(TsurugiTransaction transaction) throws IOException, TsurugiTransactionException {
        return transaction.executeAndGetList(this);
    }

    /**
     * execute query
     *
     * @param tm Transaction Manager
     * @return list of record
     * @throws IOException
     */
    @Deprecated(forRemoval = true)
    public List<R> executeAndGetList(TsurugiTransactionManager tm) throws IOException {
        return tm.executeAndGetList(this);
    }

    /**
     * execute query
     *
     * @param tm      Transaction Manager
     * @param setting transaction manager settings
     * @return list of record
     * @throws IOException
     */
    @Deprecated(forRemoval = true)
    public List<R> executeAndGetList(TsurugiTransactionManager tm, TgTmSetting setting) throws IOException {
        return tm.executeAndGetList(setting, this);
    }
}
