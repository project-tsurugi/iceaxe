package com.tsurugidb.iceaxe.transaction.manager.retry;

import java.util.function.BiFunction;

import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * Tsurugi TransactionManager retry predicate.
 */
@FunctionalInterface
public interface TsurugiTmRetryPredicate extends BiFunction<TsurugiTransaction, TsurugiTransactionException, TgTmRetryInstruction> {

}
