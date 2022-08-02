package com.tsurugidb.iceaxe.util;

import java.io.IOException;
import java.util.function.Function;

import com.tsurugidb.iceaxe.transaction.TsurugiTransactionException;

/**
 * {@link Function} with IOException, TsurugiTransactionException
 */
@FunctionalInterface
public interface TsurugiTransactionFunction<T, R> {
    public R apply(T t) throws IOException, TsurugiTransactionException;
}
