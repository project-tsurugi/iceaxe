package com.tsurugidb.iceaxe.sql.event;

import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.result.TsurugiSqlResult;
import com.tsurugidb.iceaxe.sql.result.TusurigQueryResult;
import com.tsurugidb.iceaxe.sql.result.event.TsurugiResultSetEventListener;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiSqlPreparedQuery} with {@link TsurugiSqlResult} event listener
 *
 * @param <P> parameter type
 * @param <R> result type
 */
public interface TsurugiSqlPreparedQueryResultEventListener<P, R> extends TsurugiSqlPreparedQueryEventListener<P, R> {

    @Override
    @OverridingMethodsMustInvokeSuper
    default void executeQueryStarted(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<P, R> ps, P parameter, TusurigQueryResult<R> rs) {
        rs.addEventListener(new TsurugiResultSetEventListener<>() {
            @Override
            public void readRecord(TusurigQueryResult<R> rs, R record) {
                executeQueryRead(transaction, ps, parameter, rs, record);
            }

            @Override
            public void readException(TusurigQueryResult<R> rs, Throwable occurred) {
                executeQueryException(transaction, ps, parameter, rs, occurred);
            }

            @Override
            public void endResult(TusurigQueryResult<R> rs) {
                executeQueryEnd(transaction, ps, parameter, rs);
            }

            @Override
            public void closeResult(TusurigQueryResult<R> rs, @Nullable Throwable occurred) {
                executeQueryClose(transaction, ps, parameter, rs, occurred);
            }
        });

        executeQueryStarted2(transaction, ps, parameter, rs);
    }

    /**
     * called when execute query started
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param parameter   SQL parameter
     * @param rs          ResultSet
     */
    default void executeQueryStarted2(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<P, R> ps, P parameter, TusurigQueryResult<R> rs) {
        // do override
    }

    /**
     * called when execute query read record
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param parameter   SQL parameter
     * @param rs          ResultSet
     * @param record      record
     */
    default void executeQueryRead(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<P, R> ps, P parameter, TusurigQueryResult<R> rs, R record) {
        // do override
    }

    /**
     * called when execute query read error
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param parameter   SQL parameter
     * @param rs          ResultSet
     * @param occurred    exception
     */
    default void executeQueryException(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<P, R> ps, P parameter, TusurigQueryResult<R> rs, Throwable occurred) {
        // do override
    }

    /**
     * called when execute query end
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param parameter   SQL parameter
     * @param rs          ResultSet
     */
    default void executeQueryEnd(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<P, R> ps, P parameter, TusurigQueryResult<R> rs) {
        // do override
    }

    /**
     * called when close ResultSet
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param parameter   SQL parameter
     * @param rs          ResultSet
     * @param occurred    exception
     */
    default void executeQueryClose(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<P, R> ps, P parameter, TusurigQueryResult<R> rs, @Nullable Throwable occurred) {
        // do override
    }
}
