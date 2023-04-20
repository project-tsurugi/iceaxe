package com.tsurugidb.iceaxe.util.function;

import java.io.IOException;

/**
 * {@link Runnable} with IOException
 */
@FunctionalInterface
public interface IoRunnable {
    void run() throws IOException, InterruptedException;
}
