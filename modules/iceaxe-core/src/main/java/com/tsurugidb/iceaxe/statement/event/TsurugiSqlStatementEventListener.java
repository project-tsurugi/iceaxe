package com.tsurugidb.iceaxe.statement.event;

import com.tsurugidb.iceaxe.result.TsurugiResultCount;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate0;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiPreparedStatementUpdate0} event listener
 */
public interface TsurugiSqlStatementEventListener {

    /**
     * called when execute statement start
     *
     * @param transaction transaction
     * @param ps          SQL statement
     */
    default void executeStatementStart(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate0 ps) {
        // do override
    }

    /**
     * called when execute statement started
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param rc          ResultCount
     */
    default void executeStatementStarted(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate0 ps, TsurugiResultCount rc) {
        // do override
    }
}
