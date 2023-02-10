package com.tsurugidb.iceaxe.statement.event;

import com.tsurugidb.iceaxe.result.TsurugiResultSet;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery1;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiPreparedStatementQuery1} event listener
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
    default void executeQueryStart(TsurugiTransaction transaction, TsurugiPreparedStatementQuery1<P, R> ps, P parameter, int iceaxeSqlExecuteId) {
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
    default void executeQueryStartException(TsurugiTransaction transaction, TsurugiPreparedStatementQuery1<P, R> ps, P parameter, int iceaxeSqlExecuteId, Throwable occurred) {
        // do override
    }

    /**
     * called when execute query started
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param parameter   SQL parameter
     * @param rs          ResultSet
     */
    default void executeQueryStarted(TsurugiTransaction transaction, TsurugiPreparedStatementQuery1<P, R> ps, P parameter, TsurugiResultSet<R> rs) {
        // do override
    }
}
