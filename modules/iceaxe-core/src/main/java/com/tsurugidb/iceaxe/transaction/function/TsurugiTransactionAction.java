package com.tsurugidb.iceaxe.transaction.function;

import java.io.IOException;

import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * Tsurugi transaction action.
 */
@FunctionalInterface
public interface TsurugiTransactionAction {
    /**
     * execute transaction.
     *
     * @param transaction transaction
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public void run(TsurugiTransaction transaction) throws IOException, InterruptedException, TsurugiTransactionException;
}
