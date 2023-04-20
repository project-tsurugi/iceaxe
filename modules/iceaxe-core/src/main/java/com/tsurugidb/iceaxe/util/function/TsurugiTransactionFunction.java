package com.tsurugidb.iceaxe.util.function;

import java.io.IOException;
import java.util.function.Function;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * {@link Function} with IOException, TsurugiTransactionException
 */
@FunctionalInterface
public interface TsurugiTransactionFunction<T, R> {
    R apply(T t) throws IOException, InterruptedException, TsurugiTransactionException;
}
