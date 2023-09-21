package com.tsurugidb.iceaxe.util.function;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * {@link Supplier} with IOException.
 *
 * @param <T> the type of results supplied by this supplier
 */
@FunctionalInterface
public interface IoSupplier<T> {
    /**
     * Gets a result.
     *
     * @return a result
     * @throws IOException
     * @throws InterruptedException
     */
    T get() throws IOException, InterruptedException;
}
