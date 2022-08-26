package com.tsurugidb.iceaxe.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;

/**
 * Iceaxe Closeable set
 */
@ThreadSafe
public class IceaxeCloseableSet {
    private static final Logger LOG = LoggerFactory.getLogger(IceaxeCloseableSet.class);

    private final List<AutoCloseable> closeableList = new ArrayList<>();

    /**
     * add Closeable
     * 
     * @param closeable Closeable
     */
    public synchronized void add(AutoCloseable closeable) {
        closeableList.add(closeable);
    }

    /**
     * remove Closeable
     * 
     * @param closeable Closeable
     */
    public synchronized void remove(AutoCloseable closeable) {
        closeableList.remove(closeable);
    }

    /**
     * close all Closeable
     * 
     * @return Exception list if close error occurs
     */
    public synchronized List<Throwable> close() {
        List<Throwable> result = null;
        for (var i = closeableList.iterator(); i.hasNext();) {
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
     * close all Closeable
     * 
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public synchronized void closeInTransaction() throws IOException, TsurugiTransactionException {
        if (closeableList.isEmpty()) {
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
            } else {
                throw new AssertionError(e);
            }
        }

        LOG.trace("close end");
    }

    int size() {
        return closeableList.size();
    }
}
