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
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public R run(TsurugiTransaction transaction) throws IOException, InterruptedException, TsurugiTransactionException;
}
