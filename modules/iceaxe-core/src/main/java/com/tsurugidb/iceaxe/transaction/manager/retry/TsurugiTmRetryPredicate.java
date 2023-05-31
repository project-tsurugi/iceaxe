package com.tsurugidb.iceaxe.transaction.manager.retry;

import java.util.function.BiFunction;

import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * Tsurugi TransactionManager retry predicate.
 */
@FunctionalInterface
public interface TsurugiTmRetryPredicate extends BiFunction<TsurugiTransaction, TsurugiDiagnosticCodeProvider, TgTmRetryInstruction> {

}
