package com.tsurugi.iceaxe.transaction;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;

import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos.ResultOnly;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos.ResultOnly.ResultCase;
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
    private boolean finished = false;

    // internal
    public TsurugiTransaction(TsurugiSession session, Future<Transaction> lowTransactionFuture) {
        this.ownerSession = session;
        this.lowTransactionFuture = lowTransactionFuture;
    }

    protected final TgSessionInfo getSessionInfo() {
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
     * commit
     * 
     * @throws IOException
     */
    public synchronized void commit() throws IOException {
        finish(Transaction::commit);
    }

    /**
     * rollback
     * 
     * @throws IOException
     */
    public synchronized void rollback() throws IOException {
        finish(Transaction::rollback);
    }

    protected void finish(IoFunction<Transaction, Future<ResultOnly>> finisher) throws IOException {
        if (this.finished) {
            throw new IllegalStateException("commit/rollback has already been called");
        }
        var lowResultFuture = finisher.apply(getLowTransaction());
        var info = getSessionInfo();
        var lowResult = IceaxeIoUtil.getFromFuture(lowResultFuture, info);
        if (lowResult.getResultCase() != ResultCase.SUCCESS) {
            throw new IOException(); // TODO Exception
        }
        this.finished = true;
    }

    @Override
    public void close() throws IOException {
        // not try-finally
        getLowTransaction().close();
        ownerSession.removeChild(this);
    }
}
