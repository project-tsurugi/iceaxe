package com.tsurugidb.iceaxe.session.event;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlStatement;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;

/**
 * {@link TsurugiSession} event listener.
 */
public interface TsurugiSessionEventListener {

    /**
     * called when create query.
     *
     * @param <R> result type
     * @param ps  query
     */
    default <R> void createQuery(TsurugiSqlQuery<R> ps) {
        // do override
    }

    /**
     * called when create prepared query.
     *
     * @param <P> parameter type
     * @param <R> result type
     * @param ps  query
     */
    default <P, R> void createQuery(TsurugiSqlPreparedQuery<P, R> ps) {
        // do override
    }

    /**
     * called when create statement.
     *
     * @param ps statement
     */
    default void createStatement(TsurugiSqlStatement ps) {
        // do override
    }

    /**
     * called when create prepared statement.
     *
     * @param <P> parameter type
     * @param ps  statement
     */
    default <P> void createStatement(TsurugiSqlPreparedStatement<P> ps) {
        // do override
    }

    /**
     * called when create transaction manager.
     *
     * @param tm transaction manager
     */
    default void createTransactionManager(TsurugiTransactionManager tm) {
        // do override
    }

    /**
     * called when create transaction.
     *
     * @param transaction transaction
     */
    default void createTransaction(TsurugiTransaction transaction) {
        // do override
    }

    /**
     * called when close session.
     *
     * @param session      session
     * @param timeoutNanos close timeout
     * @param occurred     exception
     */
    default void closeSession(TsurugiSession session, long timeoutNanos, @Nullable Throwable occurred) {
        // do override
    }
}
