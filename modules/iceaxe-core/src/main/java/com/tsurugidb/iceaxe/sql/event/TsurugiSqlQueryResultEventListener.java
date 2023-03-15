package com.tsurugidb.iceaxe.sql.event;

import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.result.TsurugiSqlResult;
import com.tsurugidb.iceaxe.sql.result.TusurigQueryResult;
import com.tsurugidb.iceaxe.sql.result.event.TsurugiResultSetEventListener;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiSqlQuery} with {@link TsurugiSqlResult} event listener
 *
 * @param <R> result type
 */
public interface TsurugiSqlQueryResultEventListener<R> extends TsurugiSqlQueryEventListener<R> {

    @Override
    @OverridingMethodsMustInvokeSuper
    default void executeQueryStarted(TsurugiTransaction transaction, TsurugiSqlQuery<R> ps, TusurigQueryResult<R> rs) {
        rs.addEventListener(new TsurugiResultSetEventListener<>() {
            @Override
            public void readRecord(TusurigQueryResult<R> rs, R record) {
                executeQueryRead(transaction, ps, rs, record);
            }

            @Override
            public void readException(TusurigQueryResult<R> rs, Throwable occurred) {
                executeQueryException(transaction, ps, rs, occurred);
            }

            @Override
            public void endResult(TusurigQueryResult<R> rs) {
                executeQueryEnd(transaction, ps, rs);
            }

            @Override
            public void closeResult(TusurigQueryResult<R> rs, @Nullable Throwable occurred) {
                executeQueryClose(transaction, ps, rs, occurred);
            }
        });

        executeQueryStarted2(transaction, ps, rs);
    }

    /**
     * called when execute query started
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param rs          ResultSet
     */
    default void executeQueryStarted2(TsurugiTransaction transaction, TsurugiSqlQuery<R> ps, TusurigQueryResult<R> rs) {
        // do override
    }

    /**
     * called when execute query read record
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param rs          SQL parameter
     * @param record      record
     */
    default void executeQueryRead(TsurugiTransaction transaction, TsurugiSqlQuery<R> ps, TusurigQueryResult<R> rs, R record) {
        // do override
    }

    /**
     * called when execute query read error
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param rs          SQL parameter
     * @param occurred    exception
     */
    default void executeQueryException(TsurugiTransaction transaction, TsurugiSqlQuery<R> ps, TusurigQueryResult<R> rs, Throwable occurred) {
        // do override
    }

    /**
     * called when execute query end
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param rs          SQL parameter
     */
    default void executeQueryEnd(TsurugiTransaction transaction, TsurugiSqlQuery<R> ps, TusurigQueryResult<R> rs) {
        // do override
    }

    /**
     * called when close ResultSet
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param rs          SQL parameter
     * @param occurred    exception
     */
    default void executeQueryClose(TsurugiTransaction transaction, TsurugiSqlQuery<R> ps, TusurigQueryResult<R> rs, @Nullable Throwable occurred) {
        // do override
    }
}
