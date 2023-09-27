package com.tsurugidb.iceaxe.util.function;

import java.io.IOException;
import java.util.function.Function;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * {@link Function} with IOException, TsurugiTransactionException.
 *
 * @param <T> the type of the input to the function
 * @param <R> the type of the result of the function
 */
@FunctionalInterface
public interface TsurugiTransactionFunction<T, R> {
    /**
     * Applies this function to the given argument.
     *
     * @param t the function argument
     * @return the function result
     * @throws IOException                 if an I/O error occurs while execute
     * @throws InterruptedException        if interrupted while execute
     * @throws TsurugiTransactionException if server error occurs while execute
     */
    R apply(T t) throws IOException, InterruptedException, TsurugiTransactionException;
}
