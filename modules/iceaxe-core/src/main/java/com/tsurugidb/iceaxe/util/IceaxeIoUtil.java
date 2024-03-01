package com.tsurugidb.iceaxe.util;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.function.IoRunnable;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Iceaxe I/O utility.
 */
public final class IceaxeIoUtil {

    private IceaxeIoUtil() {
        // don't instantiate
    }

    /**
     * get value from future.
     *
     * @param <V>                   the result value type
     * @param future                future
     * @param timeout               the maximum time to wait
     * @param timeoutErrorCode      error code for timeout
     * @param closeTimeoutErrorCode error code for close timeout
     * @return result value
     * @throws IOException          if an I/O error occurs while processing the request
     * @throws InterruptedException if interrupted while processing the request
     */
    public static <V> V getAndCloseFuture(FutureResponse<V> future, IceaxeTimeout timeout, IceaxeErrorCode timeoutErrorCode, IceaxeErrorCode closeTimeoutErrorCode)
            throws IOException, InterruptedException {
        return getAndCloseFuture(future, timeout, timeoutErrorCode, closeTimeoutErrorCode, TsurugiIOException::new);
    }

    /**
     * get value from future in transaction.
     *
     * @param <V>                   the result value type
     * @param future                future
     * @param timeout               the maximum time to wait
     * @param timeoutErrorCode      error code for timeout
     * @param closeTimeoutErrorCode error code for close timeout
     * @return result value
     * @throws IOException                 if an I/O error occurs while processing the request
     * @throws InterruptedException        if interrupted while processing the request
     * @throws TsurugiTransactionException if server error occurs while processing the request
     */
    public static <V> V getAndCloseFutureInTransaction(FutureResponse<V> future, IceaxeTimeout timeout, IceaxeErrorCode timeoutErrorCode, IceaxeErrorCode closeTimeoutErrorCode)
            throws IOException, InterruptedException, TsurugiTransactionException {
        return getAndCloseFuture(future, timeout, timeoutErrorCode, closeTimeoutErrorCode, TsurugiTransactionException::new);
    }

    private static <V, E extends Exception> V getAndCloseFuture(FutureResponse<V> future, IceaxeTimeout timeout, IceaxeErrorCode timeoutErrorCode, IceaxeErrorCode closeTimeoutErrorCode,
            Function<ServerException, E> serverExceptionWrapper) throws IOException, InterruptedException, E {
        Throwable occurred = null;
        try {
            var time = timeout.get();
            long value = time.value();
            TimeUnit unit = time.unit();
            return future.get(value, unit);
        } catch (ServerException e) {
            E wrapper = serverExceptionWrapper.apply(e);
            occurred = wrapper;
            throw wrapper;
        } catch (TimeoutException | ResponseTimeoutException e) {
            var ioe = new IceaxeIOException(timeoutErrorCode, e);
            occurred = ioe;
            throw ioe;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            try {
                future.close();
            } catch (ServerException e) {
                E wrapper = serverExceptionWrapper.apply(e);
                if (occurred != null) {
                    occurred.addSuppressed(wrapper);
                } else {
                    throw wrapper;
                }
            } catch (ResponseTimeoutException e) {
                var ie = new IceaxeIOException(closeTimeoutErrorCode, e);
                if (occurred != null) {
                    occurred.addSuppressed(ie);
                } else {
                    throw ie;
                }
            } catch (Throwable e) {
                if (occurred != null) {
                    occurred.addSuppressed(e);
                } else {
                    throw e;
                }
            }
        }
    }

    /**
     * Closeable for FutureResponse.
     */
    public interface IceaxeFutureResponseCloseable extends AutoCloseable {
        @Override
        void close() throws IOException, InterruptedException /* , TsurugiIOException */;
    }

    /**
     * wrap with Closeable.
     *
     * @param future                future
     * @param closeTimeoutErrorCode error code for close timeout
     * @param closeErrorCode        error code for close
     * @return Closeable
     */
    public static IceaxeFutureResponseCloseable closeable(FutureResponse<?> future, IceaxeErrorCode closeTimeoutErrorCode, IceaxeErrorCode closeErrorCode) {
        return () -> {
            // ServerException -> TsurugiIOException
            IceaxeIoUtil.close(closeTimeoutErrorCode, closeErrorCode, future);
        };
    }

    /**
     * close resources.
     *
     * @param closeableSet   Closeable set
     * @param closeErrorCode error code for close
     * @param runnable       close action
     * @throws IOException          if an I/O error occurs while disposing the resources
     * @throws InterruptedException if interrupted while disposing the resources
     */
    public static void close(IceaxeCloseableSet closeableSet, IceaxeErrorCode closeErrorCode, IoRunnable runnable) throws IOException, InterruptedException {
        List<Throwable> saveList = closeableSet.close();

        try {
            runnable.run();
        } catch (Exception e) {
            for (var save : saveList) {
                var s = (save instanceof ServerException) ? new TsurugiIOException((ServerException) save) : save;
                e.addSuppressed(s);
            }
            throw e;
        }

        IOException ioe = null;
        for (var save : saveList) {
            var s = (save instanceof ServerException) ? new TsurugiIOException((ServerException) save) : save;
            if (ioe == null) {
                if (s instanceof IOException) {
                    ioe = (IOException) s;
                } else {
                    if (hasDiagnosticCode(s)) {
                        ioe = new TsurugiIOException(s.getMessage(), s);
                    } else {
                        ioe = new IceaxeIOException(closeErrorCode, s);
                    }
                }
            } else {
                ioe.addSuppressed(s);
            }
        }
        if (ioe != null) {
            throw ioe;
        }
    }

    private static boolean hasDiagnosticCode(Throwable t) {
        if (t instanceof TsurugiDiagnosticCodeProvider) {
            return ((TsurugiDiagnosticCodeProvider) t).getDiagnosticCode() != null;
        }
        return false;
    }

    /**
     * close resources.
     *
     * @param closeTimeoutErrorCode error code for close timeout
     * @param closeErrorCode        error code for close
     * @param closeables            AutoCloseable
     * @throws IOException          if an I/O error occurs while disposing the resources
     * @throws InterruptedException if interrupted while disposing the resources
     */
    public static void close(IceaxeErrorCode closeTimeoutErrorCode, IceaxeErrorCode closeErrorCode, AutoCloseable... closeables) throws IOException, InterruptedException {
        close(closeables, closeTimeoutErrorCode, closeErrorCode, TsurugiIOException.class, TsurugiIOException::new);
    }

    /**
     * close resources in transaction.
     *
     * @param closeTimeoutErrorCode error code for close timeout
     * @param closeErrorCode        error code for close
     * @param closeables            AutoCloseable
     * @throws IOException                 if an I/O error occurs while disposing the resources
     * @throws InterruptedException        if interrupted while disposing the resources
     * @throws TsurugiTransactionException if server error occurs while disposing the resources
     */
    public static void closeInTransaction(IceaxeErrorCode closeTimeoutErrorCode, IceaxeErrorCode closeErrorCode, AutoCloseable... closeables)
            throws IOException, InterruptedException, TsurugiTransactionException {
        close(closeables, closeTimeoutErrorCode, closeErrorCode, TsurugiTransactionException.class, TsurugiTransactionException::new);
    }

    private static <E extends Exception> void close(AutoCloseable[] closeables, IceaxeErrorCode closeTimeoutErrorCode, IceaxeErrorCode closeErrorCode, Class<E> classE,
            Function<ServerException, E> serverExceptionWrapper) throws IOException, InterruptedException, E {
        Throwable occurred = null;

        for (var closeable : closeables) {
            if (closeable == null) {
                continue;
            }

            try {
                closeable.close();
            } catch (ServerException e) {
                var wrapper = serverExceptionWrapper.apply(e);
                if (occurred == null) {
                    occurred = wrapper;
                } else {
                    occurred.addSuppressed(wrapper);
                }
            } catch (ResponseTimeoutException e) {
                var ie = new IceaxeIOException(closeTimeoutErrorCode, e);
                if (occurred == null) {
                    occurred = ie;
                } else {
                    occurred.addSuppressed(ie);
                }
            } catch (IOException | InterruptedException | RuntimeException | Error e) {
                if (occurred == null) {
                    occurred = e;
                } else {
                    occurred.addSuppressed(e);
                }
            } catch (Throwable e) {
                if (occurred == null) {
                    if (hasDiagnosticCode(e)) {
                        occurred = new TsurugiIOException(e.getMessage(), e);
                    } else {
                        occurred = new IceaxeIOException(closeErrorCode, e);
                    }
                } else {
                    occurred.addSuppressed(e);
                }
            }
        }

        if (occurred != null) {
            if (classE.isInstance(occurred)) {
                throw classE.cast(occurred);
            }
            if (occurred instanceof RuntimeException) {
                throw (RuntimeException) occurred;
            }
            if (occurred instanceof Error) {
                throw (Error) occurred;
            }
            if (occurred instanceof InterruptedException) {
                throw (InterruptedException) occurred;
            }
            throw (IOException) occurred;
        }
    }
}
