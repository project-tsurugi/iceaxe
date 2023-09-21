package com.tsurugidb.iceaxe.sql.event;

import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.sql.result.event.TsurugiQueryResultEventListener;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiSqlPreparedQuery} with {@link TsurugiQueryResult} event listener.
 *
 * @param <P> parameter type
 * @param <R> result type
 */
public interface TsurugiSqlPreparedQueryResultEventListener<P, R> extends TsurugiSqlPreparedQueryEventListener<P, R> {

    @Override
    @OverridingMethodsMustInvokeSuper
    default void executeQueryStarted(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<P, R> ps, P parameter, TsurugiQueryResult<R> result) {
        result.addEventListener(new TsurugiQueryResultEventListener<>() {
            @Override
            public void readRecord(TsurugiQueryResult<R> result, R record) {
                executeQueryRead(transaction, ps, parameter, result, record);
            }

            @Override
            public void readException(TsurugiQueryResult<R> result, Throwable occurred) {
                executeQueryException(transaction, ps, parameter, result, occurred);
            }

            @Override
            public void endResult(TsurugiQueryResult<R> result) {
                executeQueryEnd(transaction, ps, parameter, result);
            }

            @Override
            public void closeResult(TsurugiQueryResult<R> result, @Nullable Throwable occurred) {
                executeQueryClose(transaction, ps, parameter, result, occurred);
            }
        });

        executeQueryStarted2(transaction, ps, parameter, result);
    }

    /**
     * called when execute query started.
     *
     * @param transaction transaction
     * @param ps          SQL definition
     * @param parameter   SQL parameter
     * @param result      SQL result
     */
    default void executeQueryStarted2(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<P, R> ps, P parameter, TsurugiQueryResult<R> result) {
        // do override
    }

    /**
     * called when execute query read record.
     *
     * @param transaction transaction
     * @param ps          SQL definition
     * @param parameter   SQL parameter
     * @param result      SQL result
     * @param record      record
     */
    default void executeQueryRead(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<P, R> ps, P parameter, TsurugiQueryResult<R> result, R record) {
        // do override
    }

    /**
     * called when execute query read error.
     *
     * @param transaction transaction
     * @param ps          SQL definition
     * @param parameter   SQL parameter
     * @param result      SQL result
     * @param occurred    exception
     */
    default void executeQueryException(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<P, R> ps, P parameter, TsurugiQueryResult<R> result, Throwable occurred) {
        // do override
    }

    /**
     * called when execute query end.
     *
     * @param transaction transaction
     * @param ps          SQL definition
     * @param parameter   SQL parameter
     * @param result      SQL result
     */
    default void executeQueryEnd(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<P, R> ps, P parameter, TsurugiQueryResult<R> result) {
        // do override
    }

    /**
     * called when close result.
     *
     * @param transaction transaction
     * @param ps          SQL definition
     * @param parameter   SQL parameter
     * @param result      SQL result
     * @param occurred    exception
     */
    default void executeQueryClose(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<P, R> ps, P parameter, TsurugiQueryResult<R> result, @Nullable Throwable occurred) {
        // do override
    }
}
