package com.tsurugidb.iceaxe.util;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * {@link Consumer} with IOException
 */
@FunctionalInterface
public interface IoConsumer<T> {
    public void accept(T t) throws IOException;
}
