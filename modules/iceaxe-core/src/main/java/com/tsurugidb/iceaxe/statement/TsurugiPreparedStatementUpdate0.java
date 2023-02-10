package com.tsurugidb.iceaxe.statement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.result.TsurugiResultCount;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.event.TsurugiSqlStatementEventListener;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;

/**
 * Tsurugi SQL statement (insert/update/delete, DDL)
 */
//TODO rename to TsurugiSqlStatement
public class TsurugiPreparedStatementUpdate0 extends TsurugiSqlDirect {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiPreparedStatementUpdate0.class);

    private List<TsurugiSqlStatementEventListener> eventListenerList = null;

    // internal
    public TsurugiPreparedStatementUpdate0(TsurugiSession session, String sql) throws IOException {
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
     * @return result
     * @throws IOException
     * @throws TsurugiTransactionException
     * @see TsurugiTransaction#executeStatement(TsurugiPreparedStatementUpdate0)
     */
    public TsurugiResultCount execute(TsurugiTransaction transaction) throws IOException, TsurugiTransactionException {
        checkClose();

        LOG.trace("executeStatement start");
        int sqlExecuteId = getNewIceaxeSqlExecuteId();
        event(null, listener -> listener.executeStatementStart(transaction, this, sqlExecuteId));

        TsurugiResultCount rc;
        try {
            var lowResultFuture = transaction.executeLow(lowTransaction -> lowTransaction.executeStatement(sql));
            LOG.trace("executeStatement started");

            rc = new TsurugiResultCount(sqlExecuteId, transaction, lowResultFuture);
        } catch (Throwable e) {
            event(e, listener -> listener.executeStatementStartException(transaction, this, sqlExecuteId, e));
            throw e;
        }

        event(null, listener -> listener.executeStatementStarted(transaction, this, rc));
        return rc;
    }

    /**
     * execute statement
     *
     * @param transaction Transaction
     * @return row count
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Deprecated(forRemoval = true)
    public int executeAndGetCount(TsurugiTransaction transaction) throws IOException, TsurugiTransactionException {
        return transaction.executeAndGetCount(this);
    }

    /**
     * execute statement
     *
     * @param tm Transaction Manager
     * @return row count
     * @throws IOException
     */
    @Deprecated(forRemoval = true)
    public int executeAndGetCount(TsurugiTransactionManager tm) throws IOException {
        return tm.executeAndGetCount(this);
    }

    /**
     * execute statement
     *
     * @param tm      Transaction Manager
     * @param setting transaction manager settings
     * @return row count
     * @throws IOException
     */
    @Deprecated(forRemoval = true)
    public int executeAndGetCount(TsurugiTransactionManager tm, TgTmSetting setting) throws IOException {
        return tm.executeAndGetCount(setting, this);
    }
}
