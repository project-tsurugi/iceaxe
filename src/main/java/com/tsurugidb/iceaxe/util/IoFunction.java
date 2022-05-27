package com.tsurugidb.iceaxe.util;

import java.io.IOException;
import java.util.function.Function;

/**
 * {@link Function} with IOException
 */
@FunctionalInterface
public interface IoFunction<T, R> {
    public R apply(T t) throws IOException;
}
