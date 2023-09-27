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
     * @throws IOException          if an I/O error occurs while execute
     * @throws InterruptedException if interrupted while execute
     */
    void run() throws IOException, InterruptedException;
}
