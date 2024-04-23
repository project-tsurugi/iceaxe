package com.tsurugidb.iceaxe.sql.event;

import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.iceaxe.sql.TsurugiSqlStatement;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.sql.result.event.TsurugiStatementResultEventListener;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiSqlStatement} with {@link TsurugiStatementResult} event listener.
 */
public interface TsurugiSqlStatementResultEventListener extends TsurugiSqlStatementEventListener {

    @Override
    @OverridingMethodsMustInvokeSuper
    default void executeStatementStarted(TsurugiTransaction transaction, TsurugiSqlStatement ps, TsurugiStatementResult result) {
        result.addEventListener(new TsurugiStatementResultEventListener() {
            @Override
            public void endResult(TsurugiStatementResult result, @Nullable Throwable occurred) {
                executeStatementEnd(transaction, ps, result, occurred);
            }

            @Override
            public void closeResult(TsurugiStatementResult result, long timeoutNanos, @Nullable Throwable occurred) {
                executeStatementClose(transaction, ps, result, timeoutNanos, occurred);
            }
        });

        executeStatementStarted2(transaction, ps, result);
    }

    /**
     * called when execute statement started.
     *
     * @param transaction transaction
     * @param ps          SQL definition
     * @param result      SQL result
     */
    default void executeStatementStarted2(TsurugiTransaction transaction, TsurugiSqlStatement ps, TsurugiStatementResult result) {
        // do override
    }

    /**
     * called when execute statement end.
     *
     * @param transaction transaction
     * @param ps          SQL definition
     * @param result      SQL result
     * @param occurred    exception
     */
    default void executeStatementEnd(TsurugiTransaction transaction, TsurugiSqlStatement ps, TsurugiStatementResult result, @Nullable Throwable occurred) {
        // do override
    }

    /**
     * called when close result.
     *
     * @param transaction  transaction
     * @param ps           SQL definition
     * @param result       SQL result
     * @param timeoutNanos close timeout
     * @param occurred     exception
     */
    default void executeStatementClose(TsurugiTransaction transaction, TsurugiSqlStatement ps, TsurugiStatementResult result, long timeoutNanos, @Nullable Throwable occurred) {
        // do override
    }
}
