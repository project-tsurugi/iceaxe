package com.tsurugidb.iceaxe.util.function;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * {@link Supplier} with IOException
 */
@FunctionalInterface
public interface IoSupplier<T> {
    T get() throws IOException, InterruptedException;
}
