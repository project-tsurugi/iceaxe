package com.tsurugidb.iceaxe.transaction.manager.retry;

import java.io.IOException;

import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * Tsurugi TransactionManager retry predicate.
 */
@FunctionalInterface
public interface TsurugiTmRetryPredicate {

    /**
     * Applies this function to the given arguments.
     *
     * @param transaction transaction
     * @param e           exception
     * @return retry instruction
     * @throws IOException
     * @throws InterruptedException
     */
    TgTmRetryInstruction apply(TsurugiTransaction transaction, TsurugiTransactionException e) throws IOException, InterruptedException;
}
