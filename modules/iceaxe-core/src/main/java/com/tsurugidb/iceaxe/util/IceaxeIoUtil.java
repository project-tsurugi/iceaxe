/*
 * Copyright 2023-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.iceaxe.util;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.exception.IceaxeTimeoutIOException;
import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.Timeout;
import com.tsurugidb.tsubakuro.util.Timeout.Policy;

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

        long start = System.nanoTime();
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
            var ioe = new IceaxeTimeoutIOException(timeoutErrorCode, e);
            occurred = ioe;
            throw ioe;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            try {
                long closeTimeout = calculateTimeoutNanos(timeout.getNanos(), start);
                future.setCloseTimeout(new Timeout(closeTimeout, TimeUnit.NANOSECONDS, Policy.ERROR));
                future.close();
            } catch (ServerException e) {
                E wrapper = serverExceptionWrapper.apply(e);
                if (occurred != null) {
                    occurred.addSuppressed(wrapper);
                } else {
                    throw wrapper;
                }
            } catch (ResponseTimeoutException e) {
                var ie = new IceaxeTimeoutIOException(closeTimeoutErrorCode, e);
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
     * @param closeTimeout          close timeout
     * @param closeTimeoutErrorCode error code for close timeout
     * @return Closeable
     */
    public static IceaxeFutureResponseCloseable closeable(FutureResponse<?> future, IceaxeTimeout closeTimeout, IceaxeErrorCode closeTimeoutErrorCode) {
        return () -> {
            closeTimeout.apply(future);
            try {
                future.close();
            } catch (ServerException e) {
                throw new TsurugiIOException(e);
            } catch (ResponseTimeoutException e) {
                throw new IceaxeTimeoutIOException(closeTimeoutErrorCode, e);
            }
        };
    }

    /**
     * close action.
     *
     * @since 1.4.0
     */
    @FunctionalInterface
    public interface IceaxeCloseAction {
        /**
         * close action.
         *
         * @param timeoutNanos close timeout
         * @throws IOException          if an I/O error occurs while disposing the resources
         * @throws InterruptedException if interrupted while disposing the resources
         */
        public void close(long timeoutNanos) throws IOException, InterruptedException;
    }

    /**
     * close resources.
     *
     * @param timeoutNanos   close timeout
     * @param closeableSet   Closeable set
     * @param closeErrorCode error code for close
     * @param closeAction    close action
     * @throws IOException          if an I/O error occurs while disposing the resources
     * @throws InterruptedException if interrupted while disposing the resources
     */
    public static void close(long timeoutNanos, IceaxeCloseableSet closeableSet, IceaxeErrorCode closeErrorCode, IceaxeCloseAction closeAction) throws IOException, InterruptedException {
        long start = System.nanoTime();
        List<Throwable> saveList = closeableSet.close(timeoutNanos);

        long timeout = calculateTimeoutNanos(timeoutNanos, start);
        try {
            closeAction.close(timeout);
        } catch (Exception e) {
            for (var save : saveList) {
                var s = (save instanceof ServerException) ? new TsurugiIOException((ServerException) save) : save;
                e.addSuppressed(s);
            }
            throw e;
        }

        IOException ioe = toIOException(saveList, closeErrorCode);
        if (ioe != null) {
            throw ioe;
        }
    }

    /**
     * convert to IOException.
     *
     * @param throwableList throwable list
     * @param errorCode     error code for IceaxeIOException
     * @return IOException
     * @since 1.8.0
     */
    public static @Nullable IOException toIOException(List<Throwable> throwableList, IceaxeErrorCode errorCode) {
        IOException ioe = null;
        for (var e : throwableList) {
            var s = (e instanceof ServerException) ? new TsurugiIOException((ServerException) e) : e;
            if (ioe == null) {
                if (s instanceof IOException) {
                    ioe = (IOException) s;
                } else {
                    if (hasDiagnosticCode(s)) {
                        ioe = new TsurugiIOException(s.getMessage(), s);
                    } else {
                        ioe = new IceaxeIOException(errorCode, s);
                    }
                }
            } else {
                ioe.addSuppressed(s);
            }
        }
        return ioe;
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
     * @param timeoutNanos          close timeout
     * @param closeTimeoutErrorCode error code for close timeout
     * @param closeErrorCode        error code for close
     * @param closeables            AutoCloseable
     * @throws IOException          if an I/O error occurs while disposing the resources
     * @throws InterruptedException if interrupted while disposing the resources
     */
    public static void close(long timeoutNanos, IceaxeErrorCode closeTimeoutErrorCode, IceaxeErrorCode closeErrorCode, AutoCloseable... closeables) throws IOException, InterruptedException {
        close(timeoutNanos, closeables, closeTimeoutErrorCode, closeErrorCode, TsurugiIOException.class, TsurugiIOException::new);
    }

    /**
     * close resources in transaction.
     *
     * @param timeoutNanos          close timeout
     * @param closeTimeoutErrorCode error code for close timeout
     * @param closeErrorCode        error code for close
     * @param closeables            AutoCloseable
     * @throws IOException                 if an I/O error occurs while disposing the resources
     * @throws InterruptedException        if interrupted while disposing the resources
     * @throws TsurugiTransactionException if server error occurs while disposing the resources
     */
    public static void closeInTransaction(long timeoutNanos, IceaxeErrorCode closeTimeoutErrorCode, IceaxeErrorCode closeErrorCode, AutoCloseable... closeables)
            throws IOException, InterruptedException, TsurugiTransactionException {
        close(timeoutNanos, closeables, closeTimeoutErrorCode, closeErrorCode, TsurugiTransactionException.class, TsurugiTransactionException::new);
    }

    private static <E extends Exception> void close(long timeoutNanos, AutoCloseable[] closeables, IceaxeErrorCode closeTimeoutErrorCode, IceaxeErrorCode closeErrorCode, Class<E> classE,
            Function<ServerException, E> serverExceptionWrapper) throws IOException, InterruptedException, E {
        Throwable occurred = null;

        long start = System.nanoTime();
        for (var closeable : closeables) {
            if (closeable == null) {
                continue;
            }

            long timeout = calculateTimeoutNanos(timeoutNanos, start);
            try {
                close(timeout, closeable);
            } catch (ServerException e) {
                var wrapper = serverExceptionWrapper.apply(e);
                occurred = addSuppressed(occurred, wrapper);
            } catch (ResponseTimeoutException e) {
                var ie = new IceaxeTimeoutIOException(closeTimeoutErrorCode, e);
                occurred = addSuppressed(occurred, ie);
            } catch (IceaxeIOException e) {
                if (occurred == null) {
                    IceaxeErrorCode code = e.getDiagnosticCode();
                    if (code.isTimeout()) {
                        if (code != closeTimeoutErrorCode) {
                            occurred = new IceaxeTimeoutIOException(closeTimeoutErrorCode, e);
                        } else {
                            occurred = e;
                        }
                    } else if (code != closeErrorCode) {
                        occurred = new IceaxeIOException(closeErrorCode, e);
                    } else {
                        occurred = e;
                    }
                } else {
                    occurred = addSuppressed(occurred, e);
                }
            } catch (IOException | InterruptedException | RuntimeException | Error e) {
                occurred = addSuppressed(occurred, e);
            } catch (Throwable e) {
                if (occurred == null) {
                    if (hasDiagnosticCode(e)) {
                        occurred = new TsurugiIOException(e.getMessage(), e);
                    } else {
                        occurred = new IceaxeIOException(closeErrorCode, e);
                    }
                } else {
                    occurred = addSuppressed(occurred, e);
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

    private static void close(long timeoutNanos, AutoCloseable closeable) throws Throwable {
        if (closeable instanceof IceaxeTimeoutCloseable) {
            var timeoutCloseable = (IceaxeTimeoutCloseable) closeable;
            timeoutCloseable.close(timeoutNanos);
            return;
        }

        if (closeable instanceof ServerResource) {
            var resource = (ServerResource) closeable;
            var timeout = new Timeout(timeoutNanos, TimeUnit.NANOSECONDS, Policy.ERROR);
            resource.setCloseTimeout(timeout);
            resource.close();
            return;
        }

        closeable.close();
    }

    private static Throwable addSuppressed(Throwable occurred, Throwable e) {
        if (occurred == null) {
            return e;
        }
        if (occurred != e) {
            occurred.addSuppressed(e);
        }
        return occurred;
    }

    /**
     * calculate timeout time.
     *
     * @param totalTimeoutNanos total timeout
     * @param start             start time
     * @return timeout
     * @since 1.4.0
     */
    public static long calculateTimeoutNanos(long totalTimeoutNanos, long start) {
        long now = System.nanoTime();
        return Math.max(totalTimeoutNanos - (now - start), TimeUnit.MILLISECONDS.toNanos(1));
    }
}
