package com.tsurugidb.iceaxe.transaction.function;

import java.io.IOException;

import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * Tsurugi transaction action with return value.
 *
 * @param <R> return type
 */
@FunctionalInterface
public interface TsurugiTransactionTask<R> {
    /**
     * execute transaction.
     *
     * @param transaction transaction
     * @return return value
     * @throws IOException                 if an I/O error occurs while execute
     * @throws InterruptedException        if interrupted while execute
     * @throws TsurugiTransactionException if server error occurs while execute
     */
    public R run(TsurugiTransaction transaction) throws IOException, InterruptedException, TsurugiTransactionException;
}
