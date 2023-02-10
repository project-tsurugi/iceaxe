package com.tsurugidb.iceaxe.transaction.manager.event;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * {@link TsurugiTransactionManager} event listener
 */
public interface TsurugiTmEventListener {

    /**
     * called when execute start
     *
     * @param tm                transaction manager
     * @param iceaxeTmExecuteId iceaxe tm executeId
     * @param option            transaction option
     */
    default void executeStart(TsurugiTransactionManager tm, int iceaxeTmExecuteId, TgTxOption option) {
        // do override
    }

    /**
     * called when before start transaction
     *
     * @param tm                transaction manager
     * @param iceaxeTmExecuteId iceaxe tm executeId
     * @param attempt           attempt number
     * @param option            transaction option
     */
    default void transactionStart(TsurugiTransactionManager tm, int iceaxeTmExecuteId, int attempt, TgTxOption option) {
        // do override
    }

    /**
     * called when started transaction
     *
     * @param transaction transaction
     */
    default void transactionStarted(TsurugiTransaction transaction) {
        // do override
    }

    /**
     * called when exception occurs in transaction
     *
     * @param transaction transaction
     * @param e           exception
     */
    default void transactionException(TsurugiTransaction transaction, Throwable e) {
        // do override
    }

    /**
     * called when rollbacked transaction
     *
     * @param transaction transaction
     * @param e           exception
     */
    default void transactionRollbacked(TsurugiTransaction transaction, Throwable e) {
        // do override
    }

    /**
     * called when transaction retrying
     *
     * @param transaction transaction
     * @param cause       exception
     * @param nextOption  next transaction option
     */
    default void transactionRetry(TsurugiTransaction transaction, Exception cause, TgTxOption nextOption) {
        // do override
    }

    /**
     * called when transaction retry over
     *
     * @param transaction transaction
     * @param cause       exception
     */
    default void transactionRetryOver(TsurugiTransaction transaction, Exception cause) {
        // do override
    }

    /**
     * called when execute success end
     *
     * @param transaction transaction
     * @param committed   {@code true} committed, {@code false} rollbacked
     * @param returnValue action return value
     */
    default void executeEndSuccess(TsurugiTransaction transaction, boolean committed, @Nullable Object returnValue) {
        // do override
    }

    /**
     * called when execute fail end
     *
     * @param tm                transaction manager
     * @param iceaxeTmExecuteId iceaxe tm executeId
     * @param option            transaction option
     * @param transaction       transaction
     * @param e                 exception
     */
    default void executeEndFail(TsurugiTransactionManager tm, int iceaxeTmExecuteId, TgTxOption option, @Nullable TsurugiTransaction transaction, Throwable e) {
        // do override
    }
}
