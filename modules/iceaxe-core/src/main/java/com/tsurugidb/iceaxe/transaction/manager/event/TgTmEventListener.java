package com.tsurugidb.iceaxe.transaction.manager.event;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * {@link TsurugiTransactionManager} event listener
 */
public class TgTmEventListener {

    /**
     * called when execute start
     *
     * @param option transaction option
     */
    public void executeStart(TgTxOption option) {
        // do override
    }

    /**
     * called when before start transaction
     *
     * @param attempt attempt number
     * @param option  transaction option
     */
    public void transactionBefore(int attempt, TgTxOption option) {
        // do override
    }

    /**
     * called when created transaction
     *
     * @param transaction transaction
     */
    public void transactionCreated(TsurugiTransaction transaction) {
        // do override
    }

    /**
     * called when exception occurs in transaction
     *
     * @param transaction transaction
     * @param e           exception
     */
    public void transactionException(TsurugiTransaction transaction, Throwable e) {
        // do override
    }

    /**
     * called when rollbacked transaction
     *
     * @param transaction transaction
     * @param e           exception
     */
    public void transactionRollbacked(TsurugiTransaction transaction, Throwable e) {
        // do override
    }

    /**
     * called when transaction retrying
     *
     * @param transaction transaction
     * @param cause       exception
     * @param nextOption  next transaction option
     */
    public void transactionRetry(TsurugiTransaction transaction, Exception cause, TgTxOption nextOption) {
        // do override
    }

    /**
     * called when transaction retry over
     *
     * @param transaction transaction
     * @param cause       exception
     */
    public void transactionRetryOver(TsurugiTransaction transaction, Exception cause) {
        // do override
    }

    /**
     * called when execute success end
     *
     * @param transaction transaction
     * @param committed   {@code true} committed, {@code false} rollbacked
     * @param returnValue action return value
     */
    public void executeEndSuccess(TsurugiTransaction transaction, boolean committed, @Nullable Object returnValue) {
        // do override
    }

    /**
     * called when execute fail end
     *
     * @param option      transaction option
     * @param transaction transaction
     * @param e           exception
     */
    public void executeEndFail(TgTxOption option, @Nullable TsurugiTransaction transaction, Throwable e) {
        // do override
    }
}
