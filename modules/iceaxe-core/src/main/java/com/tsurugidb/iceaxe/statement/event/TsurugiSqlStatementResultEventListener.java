package com.tsurugidb.iceaxe.statement.event;

import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.iceaxe.result.TsurugiResult;
import com.tsurugidb.iceaxe.result.TsurugiResultCount;
import com.tsurugidb.iceaxe.result.event.TsurugiResultCountEventListener;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate0;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiPreparedStatementUpdate0} with {@link TsurugiResult} event listener
 */
public interface TsurugiSqlStatementResultEventListener extends TsurugiSqlStatementEventListener {

    @Override
    @OverridingMethodsMustInvokeSuper
    default void executeStatementStarted(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate0 ps, TsurugiResultCount rc) {
        rc.addEventListener(new TsurugiResultCountEventListener() {
            @Override
            public void endResult(TsurugiResultCount rc, @Nullable Throwable occurred) {
                executeStatementEnd(transaction, ps, rc, occurred);
            }

            @Override
            public void closeResult(TsurugiResultCount rc, @Nullable Throwable occurred) {
                executeStatementClose(transaction, ps, rc, occurred);
            }
        });
    }

    /**
     * called when execute statement end
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param rc          ResultCount
     * @param occurred    exception
     */
    default void executeStatementEnd(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate0 ps, TsurugiResultCount rc, @Nullable Throwable occurred) {
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
    default void executeStatementClose(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate0 ps, TsurugiResultCount rc, @Nullable Throwable occurred) {
        // do override
    }
}
