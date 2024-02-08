package com.tsurugidb.iceaxe.sql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.event.TsurugiSqlStatementEventListener;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeInternal;

/**
 * Tsurugi SQL statement (insert/update/delete, DDL).
 */
public class TsurugiSqlStatement extends TsurugiSqlDirect {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiSqlStatement.class);

    private List<TsurugiSqlStatementEventListener> eventListenerList = null;

    /**
     * Creates a new instance.
     * <p>
     * Call {@link #initialize()} after construct.
     * </p>
     *
     * @param session session
     * @param sql     SQL
     */
    @IceaxeInternal
    public TsurugiSqlStatement(TsurugiSession session, String sql) {
        super(session, sql);
    }

    /**
     * add event listener.
     *
     * @param listener event listener
     * @return this
     */
    public TsurugiSql addEventListener(TsurugiSqlStatementEventListener listener) {
        if (this.eventListenerList == null) {
            this.eventListenerList = new ArrayList<>();
        }
        eventListenerList.add(listener);
        return this;
    }

    private void event(Throwable occurred, Consumer<TsurugiSqlStatementEventListener> action) {
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
     * @return SQL result
     * @throws IOException                 if an I/O error occurs while execute statement
     * @throws InterruptedException        if interrupted while execute statement
     * @throws TsurugiTransactionException if server error occurs while execute statement
     * @see TsurugiTransaction#executeStatement(TsurugiSqlStatement)
     */
    public TsurugiStatementResult execute(TsurugiTransaction transaction) throws IOException, InterruptedException, TsurugiTransactionException {
        checkClose();

        LOG.trace("execute start");
        int sqlExecuteId = getNewIceaxeSqlExecuteId();
        event(null, listener -> listener.executeStatementStart(transaction, this, sqlExecuteId));

        TsurugiStatementResult result;
        try {
            var lowResultFuture = transaction.executeLow(lowTransaction -> lowTransaction.executeStatement(sql));
            LOG.trace("execute started");

            result = new TsurugiStatementResult(sqlExecuteId, transaction, this, null, lowResultFuture);
        } catch (Throwable e) {
            event(e, listener -> listener.executeStatementStartException(transaction, this, sqlExecuteId, e));
            throw e;
        }

        event(null, listener -> listener.executeStatementStarted(transaction, this, result));
        return result;
    }
}
