package com.tsurugidb.iceaxe.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * Iceaxe Closeable set.
 */
@ThreadSafe
public class IceaxeCloseableSet {
    private static final Logger LOG = LoggerFactory.getLogger(IceaxeCloseableSet.class);

    private final Set<IceaxeTimeoutCloseable> closeableSet = new LinkedHashSet<>();

    /**
     * add Closeable.
     *
     * @param closeable Closeable
     */
    public synchronized void add(IceaxeTimeoutCloseable closeable) {
        closeableSet.add(closeable);
    }

    /**
     * remove Closeable.
     *
     * @param closeable Closeable
     */
    public synchronized void remove(IceaxeTimeoutCloseable closeable) {
        closeableSet.remove(closeable);
    }

    /**
     * close all Closeable.
     *
     * @param timeoutNanos timeout
     * @return Exception list if close error occurs
     */
    public synchronized List<Throwable> close(long timeoutNanos) {
        List<Throwable> result = null;
        long start = System.nanoTime();
        for (var i = closeableSet.iterator(); i.hasNext();) {
            var closeable = i.next();
            i.remove();

            long timeout = IceaxeIoUtil.calculateTimeoutNanos(timeoutNanos, start);
            try {
                closeable.close(timeout);
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
     * @param timeoutNanos   timeout
     * @param closeErrorCode error code for close
     * @throws IOException                 if an I/O error occurs while disposing the resources
     * @throws InterruptedException        if interrupted while requesting cancel
     * @throws TsurugiTransactionException if server error occurs while disposing the resources
     */
    public synchronized void closeInTransaction(long timeoutNanos, IceaxeErrorCode closeErrorCode) throws IOException, InterruptedException, TsurugiTransactionException {
        if (closeableSet.isEmpty()) {
            return;
        }

        LOG.trace("close start");

        Throwable e = null;
        var saveList = close(timeoutNanos);
        for (var save : saveList) {
            if (e == null) {
                e = convertExceptionInTransaction(save, closeErrorCode);
            } else {
                e.addSuppressed(save);
            }
        }

        if (e != null) {
            throwExceptionInTransaction(e);
        }

        LOG.trace("close end");
    }

    /**
     * convert exception.
     *
     * @param save           exception
     * @param closeErrorCode error code for close
     * @return converted exception
     * @since X.X.X
     */
    public Throwable convertExceptionInTransaction(Throwable save, IceaxeErrorCode closeErrorCode) {
        if (save instanceof IOException) {
            return save;
        } else if (save instanceof ServerException) {
            return new TsurugiTransactionException((ServerException) save);
        } else if (save instanceof TsurugiTransactionException) {
            return save;
        } else if (save instanceof TsurugiTransactionRuntimeException) {
            return save.getCause();
        } else if (save instanceof RuntimeException) {
            return save;
        } else if (save instanceof InterruptedException) {
            return save;
        } else {
            return new IceaxeIOException(closeErrorCode, save);
        }
    }

    /**
     * throw exception.
     *
     * @param e exception
     * @throws IOException                 if an I/O error occurs while disposing the resources
     * @throws InterruptedException        if interrupted while requesting cancel
     * @throws TsurugiTransactionException if server error occurs while disposing the resources
     * @since X.X.X
     */
    public void throwExceptionInTransaction(Throwable e) throws IOException, InterruptedException, TsurugiTransactionException {
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

    /**
     * get size.
     *
     * @return size
     */
    public int size() {
        return closeableSet.size();
    }

    @Override
    public String toString() {
        return "IceaxeCloseableSet(" + closeableSet.size() + ")";
    }
}
