package com.tsurugidb.iceaxe.util;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionException;

/**
 * Iceaxe I/O utility
 */
public final class IceaxeIoUtil {

    private IceaxeIoUtil() {
        // do nothing
    }

    /**
     * get value from future
     * 
     * @param <V>     the result value type
     * @param future  future
     * @param timeout the maximum time to wait
     * @return result value
     * @throws IOException
     */
    public static <V> V getFromFuture(FutureResponse<V> future, IceaxeTimeout timeout) throws IOException {
        var time = timeout.get();
        try {
            return future.get(time.value(), time.unit());
        } catch (ServerException | InterruptedException | TimeoutException e) {
            throw new IOException(e);
        }
    }

    /**
     * get value from future for transaction (commit/rollback)
     * 
     * @param <V>     the result value type
     * @param future  future
     * @param timeout the maximum time to wait
     * @return result value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public static <V> V getFromTransactionFuture(FutureResponse<V> future, IceaxeTimeout timeout) throws IOException, TsurugiTransactionException {
        var time = timeout.get();
        try {
            return future.get(time.value(), time.unit());
        } catch (ServerException e) {
            throw new TsurugiTransactionException(e);
        } catch (InterruptedException | TimeoutException e) {
            throw new IOException(e);
        }
    }

    /**
     * close resources
     * 
     * @param closeableSet Closeable set
     * @param runnable     close action
     * @throws IOException
     */
    public static void close(IceaxeCloseableSet closeableSet, IoRunnable runnable) throws IOException {
        List<Throwable> saveList = closeableSet.close();

        try {
            runnable.run();
        } catch (Exception e) {
            for (var save : saveList) {
                e.addSuppressed(save);
            }
            throw e;
        }

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
        if (e != null) {
            throw e;
        }
    }

    /**
     * close resources
     * 
     * @param closeables AutoCloseable
     * @throws IOException
     */
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