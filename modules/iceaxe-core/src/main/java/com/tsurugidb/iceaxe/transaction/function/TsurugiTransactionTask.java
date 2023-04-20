package com.tsurugidb.iceaxe.transaction.function;

import java.io.IOException;

import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * Tsurugi transaction action with return value
 */
@FunctionalInterface
public interface TsurugiTransactionTask<R> {
    public R run(TsurugiTransaction transaction) throws IOException, InterruptedException, TsurugiTransactionException;
}
