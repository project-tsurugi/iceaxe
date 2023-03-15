package com.tsurugidb.iceaxe.sql.event;

import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiSqlPreparedStatement} event listener
 *
 * @param <P> parameter type
 */
public interface TsurugiSqlPreparedStatementEventListener<P> {

    /**
     * called when execute statement start
     *
     * @param transaction        transaction
     * @param ps                 SQL statement
     * @param parameter          SQL parameter
     * @param iceaxeSqlExecuteId iceaxe SQL executeId
     */
    default void executeStatementStart(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<P> ps, P parameter, int iceaxeSqlExecuteId) {
        // do override
    }

    /**
     * called when execute statement start error
     *
     * @param transaction        transaction
     * @param ps                 SQL statement
     * @param parameter          SQL parameter
     * @param iceaxeSqlExecuteId iceaxe SQL executeId
     * @param occurred           exception
     */
    default void executeStatementStartException(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<P> ps, P parameter, int iceaxeSqlExecuteId, Throwable occurred) {
        // do override
    }

    /**
     * called when execute statement started
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param parameter   SQL parameter
     * @param rc          ResultCount
     */
    default void executeStatementStarted(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<P> ps, P parameter, TsurugiStatementResult rc) {
        // do override
    }
}
