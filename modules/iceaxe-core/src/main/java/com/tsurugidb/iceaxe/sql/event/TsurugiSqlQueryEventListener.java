package com.tsurugidb.iceaxe.sql.event;

import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiSqlQuery} event listener
 *
 * @param <R> result type
 */
public interface TsurugiSqlQueryEventListener<R> {

    /**
     * called when execute query start
     *
     * @param transaction        transaction
     * @param ps                 SQL definition
     * @param iceaxeSqlExecuteId iceaxe SQL executeId
     */
    default void executeQueryStart(TsurugiTransaction transaction, TsurugiSqlQuery<R> ps, int iceaxeSqlExecuteId) {
        // do override
    }

    /**
     * called when execute query start error
     *
     * @param transaction        transaction
     * @param ps                 SQL definition
     * @param iceaxeSqlExecuteId iceaxe SQL executeId
     * @param occurred           exception
     */
    default void executeQueryStartException(TsurugiTransaction transaction, TsurugiSqlQuery<R> ps, int iceaxeSqlExecuteId, Throwable occurred) {
        // do override
    }

    /**
     * called when execute query started
     *
     * @param transaction transaction
     * @param ps          SQL definition
     * @param result      SQL result
     */
    default void executeQueryStarted(TsurugiTransaction transaction, TsurugiSqlQuery<R> ps, TsurugiQueryResult<R> result) {
        // do override
    }
}
