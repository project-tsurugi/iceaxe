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
     * @param exception   exception
     * @return retry instruction
     * @throws IOException          if an I/O error occurs while retrieving transaction status
     * @throws InterruptedException if interrupted while retrieving transaction status
     */
    TgTmRetryInstruction apply(TsurugiTransaction transaction, TsurugiTransactionException exception) throws IOException, InterruptedException;
}
