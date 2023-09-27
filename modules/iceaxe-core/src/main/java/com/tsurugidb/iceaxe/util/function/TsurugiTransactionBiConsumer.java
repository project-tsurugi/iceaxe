package com.tsurugidb.iceaxe.util.function;

import java.io.IOException;
import java.util.function.BiConsumer;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * {@link BiConsumer} with IOException, TsurugiTransactionException.
 *
 * @param <T> the type of the first argument to the operation
 * @param <U> the type of the second argument to the operation
 */
@FunctionalInterface
public interface TsurugiTransactionBiConsumer<T, U> {
    /**
     * Performs this operation on the given arguments.
     *
     * @param t the first input argument
     * @param u the second input argument
     * @throws IOException                 if an I/O error occurs while execute
     * @throws InterruptedException        if interrupted while execute
     * @throws TsurugiTransactionException if server error occurs while execute
     */
    void accept(T t, U u) throws IOException, InterruptedException, TsurugiTransactionException;
}
