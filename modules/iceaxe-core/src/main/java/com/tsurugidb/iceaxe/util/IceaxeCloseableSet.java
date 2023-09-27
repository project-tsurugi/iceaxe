package com.tsurugidb.iceaxe.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * Iceaxe Closeable set.
 */
@ThreadSafe
public class IceaxeCloseableSet {
    private static final Logger LOG = LoggerFactory.getLogger(IceaxeCloseableSet.class);

    private final Set<AutoCloseable> closeableSet = new LinkedHashSet<>();

    /**
     * add Closeable.
     *
     * @param closeable Closeable
     */
    public synchronized void add(AutoCloseable closeable) {
        closeableSet.add(closeable);
    }

    /**
     * remove Closeable.
     *
     * @param closeable Closeable
     */
    public synchronized void remove(AutoCloseable closeable) {
        closeableSet.remove(closeable);
    }

    /**
     * close all Closeable.
     *
     * @return Exception list if close error occurs
     */
    public synchronized List<Throwable> close() {
        List<Throwable> result = null;
        for (var i = closeableSet.iterator(); i.hasNext();) {
            var closeable = i.next();
            i.remove();

            try {
                closeable.close();
            } catch (Exception e) {
                if (result == null) {
                    result = new ArrayList<>();
                }
                result.add(e);
            }
        }

        return (result != null) ? result : List.of();
    }

    /**
     * close all Closeable.
     *
     * @throws IOException                 if an I/O error occurs while disposing the resources
     * @throws InterruptedException        if interrupted while requesting cancel
     * @throws TsurugiTransactionException if server error occurs while disposing the resources
     */
    public synchronized void closeInTransaction() throws IOException, InterruptedException, TsurugiTransactionException {
        if (closeableSet.isEmpty()) {
            return;
        }

        LOG.trace("close start");

        Throwable e = null;
        var saveList = close();
        for (var save : saveList) {
            if (e == null) {
                if (save instanceof IOException) {
                    e = save;
                } else if (save instanceof ServerException) {
                    e = new TsurugiTransactionException((ServerException) save);
                } else if (save instanceof TsurugiTransactionException) {
                    e = save;
                } else if (save instanceof TsurugiTransactionRuntimeException) {
                    e = save.getCause();
                } else if (save instanceof RuntimeException) {
                    e = save;
                } else if (save instanceof InterruptedException) {
                    e = save;
                } else {
                    e = new IOException(save.getMessage(), save);
                }
            } else {
                e.addSuppressed(save);
            }
        }

        if (e != null) {
            if (e instanceof IOException) {
                throw (IOException) e;
            } else if (e instanceof TsurugiTransactionException) {
                throw (TsurugiTransactionException) e;
            } else if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else if (e instanceof InterruptedException) {
                throw (InterruptedException) e;
            } else {
                throw new AssertionError(e);
            }
        }

        LOG.trace("close end");
    }

    int size() {
        return closeableSet.size();
    }
}
