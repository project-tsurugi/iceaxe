package com.tsurugi.iceaxe.transaction;

import java.io.Closeable;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos.ResultOnly;
import com.tsurugi.iceaxe.session.TgSessionInfo;
import com.tsurugi.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugi.iceaxe.session.TsurugiSession;
import com.tsurugi.iceaxe.util.IceaxeIoUtil;
import com.tsurugi.iceaxe.util.IoFunction;
import com.tsurugi.iceaxe.util.TgTimeValue;

/**
 * Tsurugi Transaction
 */
public class TsurugiTransaction implements Closeable {

    private final TsurugiSession ownerSession;
    private Future<Transaction> lowTransactionFuture;
    private Transaction lowTransaction;
    private TgTimeValue beginTimeout;
    private TgTimeValue commitTimeout;
    private TgTimeValue rollbackTimeout;
    private boolean committed = false;
    private boolean rollbacked = false;
    private final NavigableSet<Closeable> closeableSet = new ConcurrentSkipListSet<>();

    // internal
    public TsurugiTransaction(TsurugiSession session, Future<Transaction> lowTransactionFuture) {
        this.ownerSession = session;
        this.lowTransactionFuture = lowTransactionFuture;
        session.addChild(this);
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
        this.beginTimeout = timeout;
    }

    protected TgTimeValue getBeginTimeout() {
        if (this.beginTimeout == null) {
            var info = getSessionInfo();
            this.beginTimeout = info.timeout(TgTimeoutKey.TRANSACTION_BEGIN);
        }
        return this.beginTimeout;
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
        this.commitTimeout = timeout;
    }

    protected TgTimeValue getCommitTimeout() {
        if (this.commitTimeout == null) {
            var info = getSessionInfo();
            this.commitTimeout = info.timeout(TgTimeoutKey.TRANSACTION_COMMIT);
        }
        return this.commitTimeout;
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
        this.rollbackTimeout = timeout;
    }

    protected TgTimeValue getRollbackTimeout() {
        if (this.rollbackTimeout == null) {
            var info = getSessionInfo();
            this.rollbackTimeout = info.timeout(TgTimeoutKey.TRANSACTION_ROLLBACK);
        }
        return this.rollbackTimeout;
    }

    public final TgSessionInfo getSessionInfo() {
        return ownerSession.getSessionInfo();
    }

    // internal
    public final synchronized Transaction getLowTransaction() throws IOException {
        if (this.lowTransaction == null) {
            this.lowTransaction = IceaxeIoUtil.getFromFuture(lowTransactionFuture, getBeginTimeout());
            this.lowTransactionFuture = null;
        }
        return this.lowTransaction;
    }

    /**
     * do commit
     * 
     * @throws IOException
     */
    public synchronized void commit() throws IOException {
        if (this.committed) {
            return;
        }
        if (this.rollbacked) {
            throw new IllegalStateException("rollback has already been called");
        }

        finish(Transaction::commit, getCommitTimeout());
        this.committed = true;
    }

    /**
     * do rollback
     * 
     * @throws IOException
     */
    public synchronized void rollback() throws IOException {
        if (this.committed || this.rollbacked) {
            return;
        }

        finish(Transaction::rollback, getRollbackTimeout());
        this.rollbacked = true;
    }

    protected void finish(IoFunction<Transaction, Future<ResultOnly>> finisher, TgTimeValue timeout) throws IOException {
        var lowResultFuture = finisher.apply(getLowTransaction());
        var lowResult = IceaxeIoUtil.getFromFuture(lowResultFuture, timeout);
        var lowResultCase = lowResult.getResultCase();
        switch (lowResultCase) {
        case SUCCESS:
            return;
        case ERROR:
            throw new TsurugiTransactionIOException(lowResult.getError());
        default:
            // FIXME commit/rollbackではSUCCESS,ERROR以外は返らないという想定で良いか？
            throw new AssertionError(lowResultCase);
        }
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
        if (!(this.committed || this.rollbacked)) {
            // commitやrollbackに失敗してもcloseは呼ばれるので、ここでIllegalStateException等を発生させるのは良くない
        }

        IceaxeIoUtil.close(closeableSet, () -> {
            // not try-finally
            getLowTransaction().close();
            ownerSession.removeChild(this);
        });
    }
}
