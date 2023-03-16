package com.tsurugidb.iceaxe.sql.event;

import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.sql.result.event.TsurugiStatementResultEventListener;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiSqlPreparedStatement} with {@link TsurugiStatementResult} event listener
 *
 * @param <P> parameter type
 */
public interface TsurugiSqlPreparedStatementResultEventListener<P> extends TsurugiSqlPreparedStatementEventListener<P> {

    @Override
    @OverridingMethodsMustInvokeSuper
    default void executeStatementStarted(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<P> ps, P parameter, TsurugiStatementResult result) {
        result.addEventListener(new TsurugiStatementResultEventListener() {
            @Override
            public void endResult(TsurugiStatementResult result, @Nullable Throwable occurred) {
                executeStatementEnd(transaction, ps, parameter, result, occurred);
            }

            @Override
            public void closeResult(TsurugiStatementResult result, @Nullable Throwable occurred) {
                executeStatementClose(transaction, ps, parameter, result, occurred);
            }
        });

        executeStatementStarted2(transaction, ps, parameter, result);
    }

    /**
     * called when execute statement started
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param parameter   SQL parameter
     * @param result      SQL result
     */
    default void executeStatementStarted2(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<P> ps, P parameter, TsurugiStatementResult result) {
        // do override
    }

    /**
     * called when execute statement end
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param parameter   SQL parameter
     * @param result      SQL result
     * @param occurred    exception
     */
    default void executeStatementEnd(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<P> ps, P parameter, TsurugiStatementResult result, @Nullable Throwable occurred) {
        // do override
    }

    /**
     * called when close result
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param parameter   SQL parameter
     * @param result      SQL result
     * @param occurred    exception
     */
    default void executeStatementClose(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<P> ps, P parameter, TsurugiStatementResult result, @Nullable Throwable occurred) {
        // do override
    }
}