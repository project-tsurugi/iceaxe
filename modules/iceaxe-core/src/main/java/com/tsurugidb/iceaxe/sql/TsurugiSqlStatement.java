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
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;

/**
 * Tsurugi SQL statement (insert/update/delete, DDL)
 */
public class TsurugiSqlStatement extends TsurugiSqlDirect {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiSqlStatement.class);

    private List<TsurugiSqlStatementEventListener> eventListenerList = null;

    // internal
    public TsurugiSqlStatement(TsurugiSession session, String sql) throws IOException {
        super(session, sql);
    }

    /**
     * add event listener
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
     * execute statement
     *
     * @param transaction Transaction
     * @return SQL result
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
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

    /**
     * execute statement
     *
     * @param transaction Transaction
     * @return row count
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    @Deprecated(forRemoval = true)
    public int executeAndGetCount(TsurugiTransaction transaction) throws IOException, InterruptedException, TsurugiTransactionException {
        return transaction.executeAndGetCount(this);
    }

    /**
     * execute statement
     *
     * @param tm Transaction Manager
     * @return row count
     * @throws IOException
     * @throws InterruptedException
     */
    @Deprecated(forRemoval = true)
    public int executeAndGetCount(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        return tm.executeAndGetCount(this);
    }

    /**
     * execute statement
     *
     * @param tm      Transaction Manager
     * @param setting transaction manager settings
     * @return row count
     * @throws IOException
     * @throws InterruptedException
     */
    @Deprecated(forRemoval = true)
    public int executeAndGetCount(TsurugiTransactionManager tm, TgTmSetting setting) throws IOException, InterruptedException {
        return tm.executeAndGetCount(setting, this);
    }
}
