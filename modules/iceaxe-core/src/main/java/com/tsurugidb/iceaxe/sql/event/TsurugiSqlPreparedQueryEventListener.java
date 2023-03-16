package com.tsurugidb.iceaxe.sql.event;

import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiSqlPreparedQuery} event listener
 *
 * @param <P> parameter type
 * @param <R> result type
 */
public interface TsurugiSqlPreparedQueryEventListener<P, R> {

    /**
     * called when execute query start
     *
     * @param transaction        transaction
     * @param ps                 SQL statement
     * @param parameter          SQL parameter
     * @param iceaxeSqlExecuteId iceaxe SQL executeId
     */
    default void executeQueryStart(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<P, R> ps, P parameter, int iceaxeSqlExecuteId) {
        // do override
    }

    /**
     * called when execute query start error
     *
     * @param transaction        transaction
     * @param ps                 SQL statement
     * @param parameter          SQL parameter
     * @param iceaxeSqlExecuteId iceaxe SQL executeId
     * @param occurred           exception
     */
    default void executeQueryStartException(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<P, R> ps, P parameter, int iceaxeSqlExecuteId, Throwable occurred) {
        // do override
    }

    /**
     * called when execute query started
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param parameter   SQL parameter
     * @param result      SQL result
     */
    default void executeQueryStarted(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<P, R> ps, P parameter, TsurugiQueryResult<R> result) {
        // do override
    }
}
