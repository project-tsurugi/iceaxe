package com.tsurugidb.iceaxe.sql.event;

import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.iceaxe.sql.TsurugiSqlStatement;
import com.tsurugidb.iceaxe.sql.result.TsurugiSqlResult;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.sql.result.event.TsurugiResultCountEventListener;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiSqlStatement} with {@link TsurugiSqlResult} event listener
 */
public interface TsurugiSqlStatementResultEventListener extends TsurugiSqlStatementEventListener {

    @Override
    @OverridingMethodsMustInvokeSuper
    default void executeStatementStarted(TsurugiTransaction transaction, TsurugiSqlStatement ps, TsurugiStatementResult rc) {
        rc.addEventListener(new TsurugiResultCountEventListener() {
            @Override
            public void endResult(TsurugiStatementResult rc, @Nullable Throwable occurred) {
                executeStatementEnd(transaction, ps, rc, occurred);
            }

            @Override
            public void closeResult(TsurugiStatementResult rc, @Nullable Throwable occurred) {
                executeStatementClose(transaction, ps, rc, occurred);
            }
        });

        executeStatementStarted2(transaction, ps, rc);
    }

    /**
     * called when execute statement started
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param rc          ResultCount
     */
    default void executeStatementStarted2(TsurugiTransaction transaction, TsurugiSqlStatement ps, TsurugiStatementResult rc) {
        // do override
    }

    /**
     * called when execute statement end
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param rc          ResultCount
     * @param occurred    exception
     */
    default void executeStatementEnd(TsurugiTransaction transaction, TsurugiSqlStatement ps, TsurugiStatementResult rc, @Nullable Throwable occurred) {
        // do override
    }

    /**
     * called when close ResultCount
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param rc          ResultCount
     * @param occurred    exception
     */
    default void executeStatementClose(TsurugiTransaction transaction, TsurugiSqlStatement ps, TsurugiStatementResult rc, @Nullable Throwable occurred) {
        // do override
    }
}
