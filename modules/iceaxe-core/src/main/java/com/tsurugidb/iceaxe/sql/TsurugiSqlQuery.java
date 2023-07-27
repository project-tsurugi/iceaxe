package com.tsurugidb.iceaxe.sql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

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
 * Tsurugi SQL definition (select)
 *
 * @param <R> result type
 */
public class TsurugiSqlQuery<R> extends TsurugiSqlDirect {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiSqlQuery.class);

    private final TgResultMapping<R> resultMapping;
    private List<TsurugiSqlQueryEventListener<R>> eventListenerList = null;

    /**
     * Creates a new instance.
     *
     * @param session       session
     * @param sql           SQL
     * @param resultMapping result mapping
     * @throws IOException
     */
    @IceaxeInternal
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
}
