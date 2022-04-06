package com.tsurugi.iceaxe.transaction;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;

import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.tsurugi.iceaxe.session.TsurugiSession;
import com.tsurugi.iceaxe.util.IceaxeIoUtil;

/**
 * Tsurugi Transaction
 */
public class TsurugiTransaction implements Closeable {

    private final TsurugiSession ownerSession;
    private Future<Transaction> lowTransactionFuture;
    private Transaction lowTransaction;

    // internal
    public TsurugiTransaction(TsurugiSession session, Future<Transaction> lowTransactionFuture) {
        this.ownerSession = session;
        this.lowTransactionFuture = lowTransactionFuture;
    }

    // internal
    public final Transaction getLowTransaction() throws IOException {
        if (this.lowTransaction == null) {
            var info = ownerSession.getSessionInfo();
            this.lowTransaction = IceaxeIoUtil.getFromFuture(lowTransactionFuture, info);
            this.lowTransactionFuture = null;
        }
        return this.lowTransaction;
    }

    @Override
    public void close() throws IOException {
        getLowTransaction().close();
        ownerSession.removeChild(this);
    }
}
