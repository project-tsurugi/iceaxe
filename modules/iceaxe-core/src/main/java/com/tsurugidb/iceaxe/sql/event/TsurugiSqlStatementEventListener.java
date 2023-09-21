package com.tsurugidb.iceaxe.sql.event;

import com.tsurugidb.iceaxe.sql.TsurugiSqlStatement;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiSqlStatement} event listener.
 */
public interface TsurugiSqlStatementEventListener {

    /**
     * called when execute statement start.
     *
     * @param transaction        transaction
     * @param ps                 SQL definition
     * @param iceaxeSqlExecuteId iceaxe SQL executeId
     */
    default void executeStatementStart(TsurugiTransaction transaction, TsurugiSqlStatement ps, int iceaxeSqlExecuteId) {
        // do override
    }

    /**
     * called when execute statement start error.
     *
     * @param transaction        transaction
     * @param ps                 SQL definition
     * @param iceaxeSqlExecuteId iceaxe SQL executeId
     * @param occurred           exception
     */
    default void executeStatementStartException(TsurugiTransaction transaction, TsurugiSqlStatement ps, int iceaxeSqlExecuteId, Throwable occurred) {
        // do override
    }

    /**
     * called when execute statement started.
     *
     * @param transaction transaction
     * @param ps          SQL definition
     * @param result      SQL result
     */
    default void executeStatementStarted(TsurugiTransaction transaction, TsurugiSqlStatement ps, TsurugiStatementResult result) {
        // do override
    }
}
