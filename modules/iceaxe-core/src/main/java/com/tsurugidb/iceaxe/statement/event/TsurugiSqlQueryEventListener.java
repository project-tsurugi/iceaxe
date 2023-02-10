package com.tsurugidb.iceaxe.statement.event;

import com.tsurugidb.iceaxe.result.TsurugiResultSet;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery0;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiPreparedStatementQuery0} event listener
 *
 * @param <R> result type
 */
public interface TsurugiSqlQueryEventListener<R> {

    /**
     * called when execute query start
     *
     * @param transaction        transaction
     * @param ps                 SQL statement
     * @param iceaxeSqlExecuteId iceaxe SQL executeId
     */
    default void executeQueryStart(TsurugiTransaction transaction, TsurugiPreparedStatementQuery0<R> ps, int iceaxeSqlExecuteId) {
        // do override
    }

    /**
     * called when execute query start error
     *
     * @param transaction        transaction
     * @param ps                 SQL statement
     * @param iceaxeSqlExecuteId iceaxe SQL executeId
     * @param occurred           exception
     */
    default void executeQueryStartException(TsurugiTransaction transaction, TsurugiPreparedStatementQuery0<R> ps, int iceaxeSqlExecuteId, Throwable occurred) {
        // do override
    }

    /**
     * called when execute query started
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param rs          ResultSet
     */
    default void executeQueryStarted(TsurugiTransaction transaction, TsurugiPreparedStatementQuery0<R> ps, TsurugiResultSet<R> rs) {
        // do override
    }
}
