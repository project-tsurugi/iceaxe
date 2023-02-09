package com.tsurugidb.iceaxe.util.function;

import java.io.IOException;
import java.util.function.Consumer;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * {@link Consumer} with IOException, TsurugiTransactionException
 */
@FunctionalInterface
public interface TsurugiTransactionConsumer<T> {
    void accept(T t) throws IOException, TsurugiTransactionException;
}
