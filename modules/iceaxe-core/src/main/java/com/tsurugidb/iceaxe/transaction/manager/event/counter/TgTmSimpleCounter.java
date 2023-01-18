package com.tsurugidb.iceaxe.transaction.manager.event.counter;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.manager.event.TgTmEventListener;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * {@link TsurugiTransactionManager} simple counter
 */
@ThreadSafe
public class TgTmSimpleCounter extends TgTmEventListener {

    private final TgTmCountAtomic counter = new TgTmCountAtomic();

    @Override
    public void executeStart(TgTxOption option) {
        counter.incrementExecuteCount();
    }

    @Override
    public void transactionBefore(int attempt, TgTxOption option) {
        counter.incrementTransactionCount();
    }

    @Override
    public void transactionCreated(TsurugiTransaction transaction) {
        transaction.addBeforeCommitListener(tx -> {
            counter.incrementBeforeCommitCount();
        });
        transaction.addCommitListener(tx -> {
            counter.incrementCommitCount();
        });
        transaction.addRollbackListener(tx -> {
            counter.incrementRollbackCount();
        });
    }

    @Override
    public void transactionException(TsurugiTransaction transaction, Throwable e) {
        counter.incrementExceptionCount();
    }

    @Override
    public void transactionRetry(TsurugiTransaction transaction, Exception cause, TgTxOption nextOption) {
        counter.incrementRetryCount();
    }

    @Override
    public void transactionRetryOver(TsurugiTransaction transaction, Exception cause) {
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
    public void executeEndFail(TgTxOption option, TsurugiTransaction transaction, Throwable e) {
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
