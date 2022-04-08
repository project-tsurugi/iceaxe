package com.tsurugi.iceaxe.util;

import java.io.IOException;

/**
 * {@link Runnable} with IOException
 */
@FunctionalInterface
public interface IoRunnable {
    public void run() throws IOException;
}
