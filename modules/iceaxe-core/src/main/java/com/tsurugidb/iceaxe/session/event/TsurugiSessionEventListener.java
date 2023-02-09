package com.tsurugidb.iceaxe.session.event;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery0;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery1;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate0;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;

/**
 * {@link TsurugiSession} event listener
 */
public interface TsurugiSessionEventListener {

    /**
     * called when create query
     *
     * @param <R> result type
     * @param ps  query
     */
    default <R> void createQuery(TsurugiPreparedStatementQuery0<R> ps) {
        // do override
    }

    /**
     * called when create prepared query
     *
     * @param <P> parameter type
     * @param <R> result type
     * @param ps  query
     */
    default <P, R> void createQuery(TsurugiPreparedStatementQuery1<P, R> ps) {
        // do override
    }

    /**
     * called when create statement
     *
     * @param ps statement
     */
    default void createStatement(TsurugiPreparedStatementUpdate0 ps) {
        // do override
    }

    /**
     * called when create prepared statement
     *
     * @param <P> parameter type
     * @param ps  statement
     */
    default <P> void createStatement(TsurugiPreparedStatementUpdate1<P> ps) {
        // do override
    }

    /**
     * called when create transaction manager
     *
     * @param tm transaction manager
     */
    default void createTransactionManager(TsurugiTransactionManager tm) {
        // do override
    }

    /**
     * called when create transaction
     *
     * @param transaction transaction
     */
    default void createTransaction(TsurugiTransaction transaction) {
        // do override
    }

    /**
     * called when close session
     *
     * @param session  session
     * @param occurred exception
     */
    default void closeSession(TsurugiSession session, Throwable occurred) {
        // do override
    }
}
