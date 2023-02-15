package com.tsurugidb.iceaxe.statement.event;

import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.iceaxe.result.TsurugiResult;
import com.tsurugidb.iceaxe.result.TsurugiResultSet;
import com.tsurugidb.iceaxe.result.event.TsurugiResultSetEventListener;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery0;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiPreparedStatementQuery0} with {@link TsurugiResult} event listener
 *
 * @param <R> result type
 */
public interface TsurugiSqlQueryResultEventListener<R> extends TsurugiSqlQueryEventListener<R> {

    @Override
    @OverridingMethodsMustInvokeSuper
    default void executeQueryStarted(TsurugiTransaction transaction, TsurugiPreparedStatementQuery0<R> ps, TsurugiResultSet<R> rs) {
        rs.addEventListener(new TsurugiResultSetEventListener<>() {
            @Override
            public void readRecord(TsurugiResultSet<R> rs, R record) {
                executeQueryRead(transaction, ps, rs, record);
            }

            @Override
            public void readException(TsurugiResultSet<R> rs, Throwable occurred) {
                executeQueryException(transaction, ps, rs, occurred);
            }

            @Override
            public void endResult(TsurugiResultSet<R> rs) {
                executeQueryEnd(transaction, ps, rs);
            }

            @Override
            public void closeResult(TsurugiResultSet<R> rs, @Nullable Throwable occurred) {
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
    default void executeQueryStarted2(TsurugiTransaction transaction, TsurugiPreparedStatementQuery0<R> ps, TsurugiResultSet<R> rs) {
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
    default void executeQueryRead(TsurugiTransaction transaction, TsurugiPreparedStatementQuery0<R> ps, TsurugiResultSet<R> rs, R record) {
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
    default void executeQueryException(TsurugiTransaction transaction, TsurugiPreparedStatementQuery0<R> ps, TsurugiResultSet<R> rs, Throwable occurred) {
        // do override
    }

    /**
     * called when execute query end
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param rs          SQL parameter
     */
    default void executeQueryEnd(TsurugiTransaction transaction, TsurugiPreparedStatementQuery0<R> ps, TsurugiResultSet<R> rs) {
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
    default void executeQueryClose(TsurugiTransaction transaction, TsurugiPreparedStatementQuery0<R> ps, TsurugiResultSet<R> rs, @Nullable Throwable occurred) {
        // do override
    }
}
