package com.tsurugidb.iceaxe.transaction;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.util.IceaxeCloseableSet;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.IoFunction;
import com.tsurugidb.iceaxe.util.TgTimeValue;

/**
 * Tsurugi Transaction
 */
public class TsurugiTransaction implements Closeable {

    private final TsurugiSession ownerSession;
    private FutureResponse<Transaction> lowTransactionFuture;
    private Transaction lowTransaction;
    private boolean calledGetLowTransaction = false;
    private final IceaxeTimeout beginTimeout;
    private final IceaxeTimeout commitTimeout;
    private final IceaxeTimeout rollbackTimeout;
    private final IceaxeTimeout closeTimeout;
    private List<Runnable> commitListenerList;
    private List<Runnable> rollbackListenerList;
    private boolean committed = false;
    private boolean rollbacked = false;
    private final IceaxeCloseableSet closeableSet = new IceaxeCloseableSet();

    // internal
    public TsurugiTransaction(TsurugiSession session, FutureResponse<Transaction> lowTransactionFuture) {
        this.ownerSession = session;
        this.lowTransactionFuture = lowTransactionFuture;
        session.addChild(this);

        var info = session.getSessionInfo();
        this.beginTimeout = new IceaxeTimeout(info, TgTimeoutKey.TRANSACTION_BEGIN);
        this.commitTimeout = new IceaxeTimeout(info, TgTimeoutKey.TRANSACTION_COMMIT);
        this.rollbackTimeout = new IceaxeTimeout(info, TgTimeoutKey.TRANSACTION_ROLLBACK);
        this.closeTimeout = new IceaxeTimeout(info, TgTimeoutKey.TRANSACTION_CLOSE);

        applyCloseTimeout();
    }

    private void applyCloseTimeout() {
        closeTimeout.apply(lowTransaction);
        closeTimeout.apply(lowTransactionFuture);
    }

    /**
     * set transaction-begin-timeout
     * 
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setBeginTimeout(long time, TimeUnit unit) {
        setBeginTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set transaction-begin-timeout
     * 
     * @param timeout time
     */
    public void setBeginTimeout(TgTimeValue timeout) {
        beginTimeout.set(timeout);
    }

    /**
     * set transaction-commit-timeout
     * 
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setCommitTimeout(long time, TimeUnit unit) {
        setCommitTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set transaction-commit-timeout
     * 
     * @param timeout time
     */
    public void setCommitTimeout(TgTimeValue timeout) {
        commitTimeout.set(timeout);
    }

    /**
     * set transaction-rollback-timeout
     * 
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setRollbackTimeout(long time, TimeUnit unit) {
        setRollbackTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set transaction-rollback-timeout
     * 
     * @param timeout time
     */
    public void setRollbackTimeout(TgTimeValue timeout) {
        rollbackTimeout.set(timeout);
    }

    /**
     * set transaction-close-timeout
     * 
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setCloseTimeout(long time, TimeUnit unit) {
        setCloseTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set transaction-close-timeout
     * 
     * @param timeout time
     */
    public void setCloseTimeout(TgTimeValue timeout) {
        closeTimeout.set(timeout);

        applyCloseTimeout();
    }

    // internal
    public final TgSessionInfo getSessionInfo() {
        return ownerSession.getSessionInfo();
    }

    // internal
//  @ThreadSafe
    public final synchronized Transaction getLowTransaction() throws IOException {
        this.calledGetLowTransaction = true;
        if (this.lowTransaction == null) {
            this.lowTransaction = IceaxeIoUtil.getFromFuture(lowTransactionFuture, beginTimeout);
            try {
                IceaxeIoUtil.close(lowTransactionFuture);
                this.lowTransactionFuture = null;
            } finally {
                applyCloseTimeout();
            }
        }
        return this.lowTransaction;
    }

    /**
     * add commit listener
     * 
     * @param listener listener
     */
    public void addCommitListener(Runnable listener) {
        if (this.commitListenerList == null) {
            this.commitListenerList = new ArrayList<>();
        }
        commitListenerList.add(listener);
    }

    /**
     * add rollback listener
     * 
     * @param listener listener
     */
    public void addRollbackListener(Runnable listener) {
        if (this.rollbackListenerList == null) {
            this.rollbackListenerList = new ArrayList<>();
        }
        rollbackListenerList.add(listener);
    }

    /**
     * Whether transaction is available
     * 
     * @return true: available
     * @throws IOException
     */
//  @ThreadSafe
    public final synchronized boolean available() throws IOException {
        if (!this.calledGetLowTransaction) {
            getLowTransaction();
        }
        return this.lowTransaction != null;
    }

    /**
     * do commit
     * 
     * @param commitType commit type
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public synchronized void commit(TgCommitType commitType) throws IOException, TsurugiTransactionException {
        if (this.committed) {
            return;
        }
        if (this.rollbacked) {
            throw new IllegalStateException("rollback has already been called");
        }

        var lowCommitStatus = commitType.getLowCommitStatus();
        finish(lowTx -> lowTx.commit(lowCommitStatus), commitTimeout);
        this.committed = true;

        if (commitListenerList != null) {
            for (var listener : commitListenerList) {
                listener.run();
            }
        }
    }

    /**
     * do rollback
     * 
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public synchronized void rollback() throws IOException, TsurugiTransactionException {
        if (this.committed || this.rollbacked) {
            return;
        }

        finish(Transaction::rollback, rollbackTimeout);
        this.rollbacked = true;

        if (rollbackListenerList != null) {
            for (var listener : rollbackListenerList) {
                listener.run();
            }
        }
    }

    protected void finish(IoFunction<Transaction, FutureResponse<Void>> finisher, IceaxeTimeout timeout) throws IOException, TsurugiTransactionException {
        var transaction = getLowTransaction();
        var lowResultFuture = finisher.apply(transaction);
        IceaxeIoUtil.checkAndCloseTransactionFuture(lowResultFuture, timeout, closeTimeout);
    }

    /**
     * get committed
     * 
     * @return true: committed
     */
    public synchronized boolean isCommitted() {
        return this.committed;
    }

    /**
     * get rollbacked
     * 
     * @return true: rollbacked
     */
    public synchronized boolean isRollbacked() {
        return this.rollbacked;
    }

    // internal
    public void addChild(Closeable closeable) {
        closeableSet.add(closeable);
    }

    // internal
    public void removeChild(Closeable closeable) {
        closeableSet.remove(closeable);
    }

    @Override
    public void close() throws IOException {
//      if (!(this.committed || this.rollbacked)) {
        // commitやrollbackに失敗してもcloseは呼ばれるので、ここでIllegalStateException等を発生させるのは良くない
//      }

        IceaxeIoUtil.close(closeableSet, () -> {
            // not try-finally
            IceaxeIoUtil.close(lowTransaction, lowTransactionFuture);
            ownerSession.removeChild(this);
        });
    }
}
