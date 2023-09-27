package com.tsurugidb.iceaxe.util.function;

import java.io.IOException;
import java.util.function.Consumer;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * {@link Consumer} with IOException, TsurugiTransactionException.
 *
 * @param <T> the type of the input to the operation
 */
@FunctionalInterface
public interface TsurugiTransactionConsumer<T> {
    /**
     * Performs this operation on the given argument.
     *
     * @param t the input argument
     * @throws IOException                 if an I/O error occurs while execute
     * @throws InterruptedException        if interrupted while execute
     * @throws TsurugiTransactionException if server error occurs while execute
     */
    void accept(T t) throws IOException, InterruptedException, TsurugiTransactionException;
}
