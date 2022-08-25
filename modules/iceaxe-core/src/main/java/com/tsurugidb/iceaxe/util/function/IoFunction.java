package com.tsurugidb.iceaxe.util.function;

import java.io.IOException;
import java.util.function.Function;

/**
 * {@link Function} with IOException
 */
@FunctionalInterface
public interface IoFunction<T, R> {
    public R apply(T t) throws IOException;
}
