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

        List<Throwable> list1 = null;
        List<Throwable> list2 = null;
        for (var i = closeableList.iterator(); i.hasNext();) {
            var closeable = i.next();
            i.remove();

            try {
                closeable.close();
            } catch (ServerException | TsurugiTransactionException | TsurugiTransactionRuntimeException e) {
                if (list1 == null) {
                    list1 = new ArrayList<>();
                }
                list1.add(e);
            } catch (Exception e) {
                if (list2 == null) {
                    list2 = new ArrayList<>();
                }
                list2.add(e);
            }
        }

        if (list1 != null) {
            TsurugiTransactionException e = null;
            for (var save : list1) {
                if (e == null) {
                    if (save instanceof ServerException) {
                        e = new TsurugiTransactionException((ServerException) save);
                    } else if (save instanceof TsurugiTransactionException) {
                        e = (TsurugiTransactionException) save;
                    } else if (save instanceof TsurugiTransactionRuntimeException) {
                        e = ((TsurugiTransactionRuntimeException) save).getCause();
                    } else {
                        throw new InternalError(save);
                    }
                } else {
                    e.addSuppressed(save);
                }
            }
            if (list2 != null) {
                for (var save : list2) {
                    e.addSuppressed(save);
                }
            }
            throw e;
        }
        if (list2 != null) {
            IOException e = null;
            for (var save : list2) {
                if (e == null) {
                    if (save instanceof IOException) {
                        e = (IOException) save;
                    } else {
                        e = new IOException(save.getMessage(), save);
                    }
                } else {
                    e.addSuppressed(save);
                }
            }
            throw e;
        }

        LOG.trace("close end");
    }

    int size() {
        return closeableList.size();
    }
}
