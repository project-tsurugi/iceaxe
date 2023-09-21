package com.tsurugidb.iceaxe.sql.event;

import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.sql.result.event.TsurugiQueryResultEventListener;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiSqlQuery} with {@link TsurugiQueryResult} event listener.
 *
 * @param <R> result type
 */
public interface TsurugiSqlQueryResultEventListener<R> extends TsurugiSqlQueryEventListener<R> {

    @Override
    @OverridingMethodsMustInvokeSuper
    default void executeQueryStarted(TsurugiTransaction transaction, TsurugiSqlQuery<R> ps, TsurugiQueryResult<R> result) {
        result.addEventListener(new TsurugiQueryResultEventListener<>() {
            @Override
            public void readRecord(TsurugiQueryResult<R> result, R record) {
                executeQueryRead(transaction, ps, result, record);
            }

            @Override
            public void readException(TsurugiQueryResult<R> result, Throwable occurred) {
                executeQueryException(transaction, ps, result, occurred);
            }

            @Override
            public void endResult(TsurugiQueryResult<R> result) {
                executeQueryEnd(transaction, ps, result);
            }

            @Override
            public void closeResult(TsurugiQueryResult<R> result, @Nullable Throwable occurred) {
                executeQueryClose(transaction, ps, result, occurred);
            }
        });

        executeQueryStarted2(transaction, ps, result);
    }

    /**
     * called when execute query started.
     *
     * @param transaction transaction
     * @param ps          SQL definition
     * @param result      SQL result
     */
    default void executeQueryStarted2(TsurugiTransaction transaction, TsurugiSqlQuery<R> ps, TsurugiQueryResult<R> result) {
        // do override
    }

    /**
     * called when execute query read record.
     *
     * @param transaction transaction
     * @param ps          SQL definition
     * @param result      SQL result
     * @param record      record
     */
    default void executeQueryRead(TsurugiTransaction transaction, TsurugiSqlQuery<R> ps, TsurugiQueryResult<R> result, R record) {
        // do override
    }

    /**
     * called when execute query read error.
     *
     * @param transaction transaction
     * @param ps          SQL definition
     * @param result      SQL result
     * @param occurred    exception
     */
    default void executeQueryException(TsurugiTransaction transaction, TsurugiSqlQuery<R> ps, TsurugiQueryResult<R> result, Throwable occurred) {
        // do override
    }

    /**
     * called when execute query end.
     *
     * @param transaction transaction
     * @param ps          SQL definition
     * @param result      SQL result
     */
    default void executeQueryEnd(TsurugiTransaction transaction, TsurugiSqlQuery<R> ps, TsurugiQueryResult<R> result) {
        // do override
    }

    /**
     * called when close result.
     *
     * @param transaction transaction
     * @param ps          SQL definition
     * @param result      SQL result
     * @param occurred    exception
     */
    default void executeQueryClose(TsurugiTransaction transaction, TsurugiSqlQuery<R> ps, TsurugiQueryResult<R> result, @Nullable Throwable occurred) {
        // do override
    }
}
