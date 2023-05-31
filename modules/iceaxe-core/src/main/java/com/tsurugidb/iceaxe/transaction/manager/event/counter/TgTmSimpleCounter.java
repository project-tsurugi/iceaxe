package com.tsurugidb.iceaxe.transaction.manager.event.counter;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.event.TsurugiTransactionEventListener;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.manager.event.TsurugiTmEventListener;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOption;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * {@link TsurugiTransactionManager} simple counter
 */
@ThreadSafe
public class TgTmSimpleCounter implements TsurugiTmEventListener {

    private final TgTmCountAtomic counter = new TgTmCountAtomic();

    @Override
    public void executeStart(TsurugiTransactionManager tm, int iceaxeTmExecuteId, TgTxOption txOption) {
        counter.incrementExecuteCount();
    }

    @Override
    public void transactionStart(TsurugiTransactionManager tm, int iceaxeTmExecuteId, int attempt, TgTxOption txOption) {
        counter.incrementTransactionCount();
    }

    @Override
    public void transactionStarted(TsurugiTransaction transaction) {
        transaction.addEventListener(new TsurugiTransactionEventListener() {
            @Override
            public void commitStart(TsurugiTransaction transaction, TgCommitType commitType) {
                counter.incrementBeforeCommitCount();
            }

            @Override
            public void commitEnd(TsurugiTransaction transaction, TgCommitType commitType, Throwable occurred) {
                if (occurred == null) {
                    counter.incrementCommitCount();
                }
            }

            @Override
            public void rollbackEnd(TsurugiTransaction transaction, Throwable occurred) {
                if (occurred == null) {
                    counter.incrementRollbackCount();
                }
            }
        });
    }

    @Override
    public void transactionException(TsurugiTransaction transaction, Throwable e) {
        counter.incrementExceptionCount();
    }

    @Override
    public void transactionRetry(TsurugiTransaction transaction, Exception cause, TgTmTxOption nextTmOption) {
        counter.incrementRetryCount();
    }

    @Override
    public void transactionRetryOver(TsurugiTransaction transaction, Exception cause, TgTmTxOption nextTmOption) {
        counter.incrementRetryOverCount();
    }

    @Override
    public void executeEndSuccess(TsurugiTransaction transaction, boolean committed, Object returnValue) {
        if (committed) {
            counter.incrementSuccessCommitCount();
        } else {
            counter.incrementSuccessRollbackCount();
        }
    }

    @Override
    public void executeEndFail(TsurugiTransactionManager tm, int iceaxeTmExecuteId, TgTxOption txOption, TsurugiTransaction transaction, Throwable e) {
        counter.incrementFailCount();
    }

    /**
     * get count
     *
     * @return count
     */
    public TgTmCount getCount() {
        return this.counter;
    }

    /**
     * reset count
     */
    public void reset() {
        counter.clear();
    }

    @Override
    public String toString() {
        return "TgTmSimpleCounter" + counter;
    }
}
