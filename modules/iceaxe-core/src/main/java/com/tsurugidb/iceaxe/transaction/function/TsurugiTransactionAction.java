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
     * @throws IOException                 if an I/O error occurs while execute
     * @throws InterruptedException        if interrupted while execute
     * @throws TsurugiTransactionException if server error occurs while execute
     */
    public void run(TsurugiTransaction transaction) throws IOException, InterruptedException, TsurugiTransactionException;
}
