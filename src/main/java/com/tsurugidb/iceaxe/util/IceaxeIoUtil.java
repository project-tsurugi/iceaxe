package com.tsurugidb.iceaxe.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeoutException;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

// internal
public class IceaxeIoUtil {

    public static <T> T getFromFuture(FutureResponse<T> future, IceaxeTimeout timeout) throws IOException {
        var time = timeout.get();
        try {
            return future.get(time.value(), time.unit());
        } catch (ServerException | InterruptedException | TimeoutException e) {
            throw new IOException(e);
        }
    }

    public static void close(NavigableSet<Closeable> closeableSet, IoRunnable runnable) throws IOException {
        List<Throwable> saveList = null;
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
            } catch (Exception e) {
                if (saveList == null) {
                    saveList = new ArrayList<>();
                }
                saveList.add(e);
            }
        }

        try {
            runnable.run();
        } catch (Exception e) {
            if (saveList != null) {
                for (var save : saveList) {
                    e.addSuppressed(save);
                }
            }
            throw e;
        }

        if (saveList != null) {
            IOException e = null;
            for (var save : saveList) {
                if (e == null) {
                    if (save instanceof IOException) {
                        e = (IOException) save;
                    } else {
                        e = new IOException(save);
                    }
                } else {
                    e.addSuppressed(save);
                }
            }
            throw e;
        }
    }

    public static void close(AutoCloseable... closeables) throws IOException {
        IOException save = null;
        for (var closeable : closeables) {
            if (closeable == null) {
                continue;
            }

            try {
                closeable.close();
            } catch (IOException e) {
                if (save == null) {
                    save = e;
                } else {
                    save.addSuppressed(e);
                }
            } catch (Exception e) {
                if (save == null) {
                    save = new IOException(e);
                } else {
                    save.addSuppressed(e);
                }
            }
        }

        if (save != null) {
            throw save;
        }
    }
}
