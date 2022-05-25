package com.tsurugi.iceaxe.transaction;

import java.io.Closeable;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Future;

import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos.ResultOnly;
import com.tsurugi.iceaxe.session.TgSessionInfo;
import com.tsurugi.iceaxe.session.TsurugiSession;
import com.tsurugi.iceaxe.util.IceaxeIoUtil;
import com.tsurugi.iceaxe.util.IoFunction;

/**
 * Tsurugi Transaction
 */
public class TsurugiTransaction implements Closeable {

    private final TsurugiSession ownerSession;
    private Future<Transaction> lowTransactionFuture;
    private Transaction lowTransaction;
    private boolean committed = false;
    private boolean rollbacked = false;
    private final NavigableSet<Closeable> closeableSet = new ConcurrentSkipListSet<>();

    // internal
    public TsurugiTransaction(TsurugiSession session, Future<Transaction> lowTransactionFuture) {
        this.ownerSession = session;
        this.lowTransactionFuture = lowTransactionFuture;
        session.addChild(this);
    }

    public final TgSessionInfo getSessionInfo() {
        return ownerSession.getSessionInfo();
    }

    // internal
    public final synchronized Transaction getLowTransaction() throws IOException {
        if (this.lowTransaction == null) {
            var info = getSessionInfo();
            this.lowTransaction = IceaxeIoUtil.getFromFuture(lowTransactionFuture, info);
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

        finish(Transaction::commit);
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

        finish(Transaction::rollback);
        this.rollbacked = true;
    }

    protected void finish(IoFunction<Transaction, Future<ResultOnly>> finisher) throws IOException {
        var lowResultFuture = finisher.apply(getLowTransaction());
        var info = getSessionInfo();
        var lowResult = IceaxeIoUtil.getFromFuture(lowResultFuture, info);
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
