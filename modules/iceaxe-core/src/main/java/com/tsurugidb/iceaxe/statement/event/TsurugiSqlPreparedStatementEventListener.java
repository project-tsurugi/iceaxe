package com.tsurugidb.iceaxe.statement.event;

import com.tsurugidb.iceaxe.result.TsurugiResultCount;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiPreparedStatementUpdate1} event listener
 *
 * @param <P> parameter type
 */
public interface TsurugiSqlPreparedStatementEventListener<P> {

    /**
     * called when execute statement start
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param parameter   SQL parameter
     */
    default void executeStatementStart(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate1<P> ps, P parameter) {
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
    default void executeStatementStarted(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate1<P> ps, P parameter, TsurugiResultCount rc) {
        // do override
    }
}
