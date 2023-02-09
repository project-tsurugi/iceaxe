package com.tsurugidb.iceaxe.result;

import java.io.IOException;

import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * Tsurugi SQL result
 */
public abstract class TsurugiResult implements AutoCloseable {

    private final TsurugiTransaction ownerTransaction;

    // internal
    public TsurugiResult(TsurugiTransaction transaction) throws IOException {
        this.ownerTransaction = transaction;
        transaction.addChild(this);
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void close() throws IOException, TsurugiTransactionException {
        ownerTransaction.removeChild(this);
    }
}
