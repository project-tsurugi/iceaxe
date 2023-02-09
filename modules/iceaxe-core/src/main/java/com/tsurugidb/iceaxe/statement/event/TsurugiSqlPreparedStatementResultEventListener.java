package com.tsurugidb.iceaxe.statement.event;

import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.iceaxe.result.TsurugiResult;
import com.tsurugidb.iceaxe.result.TsurugiResultCount;
import com.tsurugidb.iceaxe.result.event.TsurugiResultCountEventListener;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiPreparedStatementUpdate1} with {@link TsurugiResult} event listener
 *
 * @param <P> parameter type
 */
public interface TsurugiSqlPreparedStatementResultEventListener<P> extends TsurugiSqlPreparedStatementEventListener<P> {

    @Override
    @OverridingMethodsMustInvokeSuper
    default void executeStatementStarted(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate1<P> ps, P parameter, TsurugiResultCount rc) {
        rc.addEventListener(new TsurugiResultCountEventListener() {
            @Override
            public void endResult(TsurugiResultCount rc, @Nullable Throwable occurred) {
                executeStatementEnd(transaction, ps, parameter, rc, occurred);
            }

            @Override
            public void closeResult(TsurugiResultCount rc, @Nullable Throwable occurred) {
                executeStatementClose(transaction, ps, parameter, rc, occurred);
            }
        });
    }

    /**
     * called when execute statement end
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param parameter   SQL parameter
     * @param rc          ResultCount
     * @param occurred    exception
     */
    default void executeStatementEnd(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate1<P> ps, P parameter, TsurugiResultCount rc, @Nullable Throwable occurred) {
        // do override
    }

    /**
     * called when close ResultCount
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param parameter   SQL parameter
     * @param rc          ResultCount
     * @param occurred    exception
     */
    default void executeStatementClose(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate1<P> ps, P parameter, TsurugiResultCount rc, @Nullable Throwable occurred) {
        // do override
    }
}
