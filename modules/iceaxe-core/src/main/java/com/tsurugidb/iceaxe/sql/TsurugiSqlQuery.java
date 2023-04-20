package com.tsurugidb.iceaxe.sql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.event.TsurugiSqlQueryEventListener;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;

/**
 * Tsurugi SQL query (select)
 *
 * @param <R> result type
 */
public class TsurugiSqlQuery<R> extends TsurugiSqlDirect {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiSqlQuery.class);

    private final TgResultMapping<R> resultMapping;
    private List<TsurugiSqlQueryEventListener<R>> eventListenerList = null;

    // internal
    public TsurugiSqlQuery(TsurugiSession session, String sql, TgResultMapping<R> resultMapping) throws IOException {
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

    private void event(Throwable occurred, Consumer<TsurugiSqlQueryEventListener<R>> action) {
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
     * @return SQL result
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
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
            result = new TsurugiQueryResult<>(sqlExecuteId, transaction, this, null, lowResultSetFuture, resultMapping, convertUtil);
        } catch (Throwable e) {
            event(e, listener -> listener.executeQueryStartException(transaction, this, sqlExecuteId, e));
            throw e;
        }

        event(null, listener -> listener.executeQueryStarted(transaction, this, result));
        return result;
    }

    /**
     * execute query
     *
     * @param transaction Transaction
     * @return record
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    @Deprecated(forRemoval = true)
    public Optional<R> executeAndFindRecord(TsurugiTransaction transaction) throws IOException, InterruptedException, TsurugiTransactionException {
        return transaction.executeAndFindRecord(this);
    }

    /**
     * execute query
     *
     * @param tm Transaction Manager
     * @return record
     * @throws IOException
     * @throws InterruptedException
     */
    @Deprecated(forRemoval = true)
    public Optional<R> executeAndFindRecord(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        return tm.executeAndFindRecord(this);
    }

    /**
     * execute query
     *
     * @param tm      Transaction Manager
     * @param setting transaction manager settings
     * @return record
     * @throws IOException
     * @throws InterruptedException
     */
    @Deprecated(forRemoval = true)
    public Optional<R> executeAndFindRecord(TsurugiTransactionManager tm, TgTmSetting setting) throws IOException, InterruptedException {
        return tm.executeAndFindRecord(setting, this);
    }

    /**
     * execute query
     *
     * @param transaction Transaction
     * @return list of record
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    @Deprecated(forRemoval = true)
    public List<R> executeAndGetList(TsurugiTransaction transaction) throws IOException, InterruptedException, TsurugiTransactionException {
        return transaction.executeAndGetList(this);
    }

    /**
     * execute query
     *
     * @param tm Transaction Manager
     * @return list of record
     * @throws IOException
     * @throws InterruptedException
     */
    @Deprecated(forRemoval = true)
    public List<R> executeAndGetList(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        return tm.executeAndGetList(this);
    }

    /**
     * execute query
     *
     * @param tm      Transaction Manager
     * @param setting transaction manager settings
     * @return list of record
     * @throws IOException
     * @throws InterruptedException
     */
    @Deprecated(forRemoval = true)
    public List<R> executeAndGetList(TsurugiTransactionManager tm, TgTmSetting setting) throws IOException, InterruptedException {
        return tm.executeAndGetList(setting, this);
    }
}
