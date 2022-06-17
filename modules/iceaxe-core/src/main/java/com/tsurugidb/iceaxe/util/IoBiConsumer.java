package com.tsurugidb.iceaxe.util;

import java.io.IOException;
import java.util.function.BiConsumer;

/**
 * {@link BiConsumer} with IOException
 */
@FunctionalInterface
public interface IoBiConsumer<T, U> {
    public void accept(T t, U u) throws IOException;
}
