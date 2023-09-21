package com.tsurugidb.iceaxe.util.function;

import java.io.IOException;
import java.util.function.Function;

/**
 * {@link Function} with IOException.
 *
 * @param <T> the type of the input to the function
 * @param <R> the type of the result of the function
 */
@FunctionalInterface
public interface IoFunction<T, R> {
    /**
     * Applies this function to the given argument.
     *
     * @param t the function argument
     * @return the function result
     * @throws IOException
     * @throws InterruptedException
     */
    R apply(T t) throws IOException, InterruptedException;
}
