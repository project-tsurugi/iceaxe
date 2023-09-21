package com.tsurugidb.iceaxe.util.function;

import java.io.IOException;

/**
 * {@link Runnable} with IOException.
 */
@FunctionalInterface
public interface IoRunnable {
    /**
     * execute.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    void run() throws IOException, InterruptedException;
}
