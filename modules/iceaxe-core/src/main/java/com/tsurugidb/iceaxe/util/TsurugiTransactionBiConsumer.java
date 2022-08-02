package com.tsurugidb.iceaxe.util;

import java.io.IOException;
import java.util.function.BiConsumer;

import com.tsurugidb.iceaxe.transaction.TsurugiTransactionException;

/**
 * {@link BiConsumer} with IOException, TsurugiTransactionException
 */
@FunctionalInterface
public interface TsurugiTransactionBiConsumer<T, U> {
    public void accept(T t, U u) throws IOException, TsurugiTransactionException;
}
