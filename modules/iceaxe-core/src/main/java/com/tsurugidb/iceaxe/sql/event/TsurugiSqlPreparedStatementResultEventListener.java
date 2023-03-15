package com.tsurugidb.iceaxe.sql.event;

import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.result.TsurugiSqlResult;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.sql.result.event.TsurugiResultCountEventListener;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiSqlPreparedStatement} with {@link TsurugiSqlResult} event listener
 *
 * @param <P> parameter type
 */
public interface TsurugiSqlPreparedStatementResultEventListener<P> extends TsurugiSqlPreparedStatementEventListener<P> {

    @Override
    @OverridingMethodsMustInvokeSuper
    default void executeStatementStarted(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<P> ps, P parameter, TsurugiStatementResult rc) {
        rc.addEventListener(new TsurugiResultCountEventListener() {
            @Override
            public void endResult(TsurugiStatementResult rc, @Nullable Throwable occurred) {
                executeStatementEnd(transaction, ps, parameter, rc, occurred);
            }

            @Override
            public void closeResult(TsurugiStatementResult rc, @Nullable Throwable occurred) {
                executeStatementClose(transaction, ps, parameter, rc, occurred);
            }
        });

        executeStatementStarted2(transaction, ps, parameter, rc);
    }

    /**
     * called when execute statement started
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param parameter   SQL parameter
     * @param rc          ResultCount
     */
    default void executeStatementStarted2(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<P> ps, P parameter, TsurugiStatementResult rc) {
        // do override
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
    default void executeStatementEnd(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<P> ps, P parameter, TsurugiStatementResult rc, @Nullable Throwable occurred) {
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
    default void executeStatementClose(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<P> ps, P parameter, TsurugiStatementResult rc, @Nullable Throwable occurred) {
        // do override
    }
}
