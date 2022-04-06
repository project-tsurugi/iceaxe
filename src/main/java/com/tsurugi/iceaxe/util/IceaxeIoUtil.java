package com.tsurugi.iceaxe.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import com.tsurugi.iceaxe.session.TgSessionInfo;

// internal
public class IceaxeIoUtil {

    public static <T> T getFromFuture(Future<T> future, TgSessionInfo info) throws IOException {
        try {
            return future.get(info.timeoutTime(), info.timeoutUnit());
        } catch (ExecutionException e) {
            Throwable c = e.getCause();
            if (c instanceof IOException) {
                throw (IOException) c;
            }
            throw new IOException(e);
        } catch (InterruptedException | TimeoutException e) {
            throw new IOException(e);
        }
    }

    public static void close(NavigableSet<Closeable> closeableSet, IoRunnable runnable) throws IOException {
        IOException save = null;
        for (;;) {
            Closeable closeable;
            try {
                closeable = closeableSet.first();
            } catch (NoSuchElementException e) {
                break;
            }
            closeableSet.remove(closeable);
            try {
                closeable.close();
            } catch (IOException e) {
                if (save == null) {
                    save = e;
                } else {
                    save.addSuppressed(e);
                }
            }
        }

        try {
            runnable.run();
        } catch (Throwable t) {
            if (save != null) {
                t.addSuppressed(save);
            }
            throw t;
        }

        if (save != null) {
            throw save;
        }
    }
}
