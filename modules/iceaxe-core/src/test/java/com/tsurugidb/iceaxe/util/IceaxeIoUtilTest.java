/*
 * Copyright 2023-2024 Project Tsurugi.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.exception.IceaxeServerExceptionTestMock;
import com.tsurugidb.iceaxe.exception.IceaxeTimeoutIOException;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.test.low.TestFutureResponse;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;
import com.tsurugidb.tsubakuro.exception.ServerException;

class IceaxeIoUtilTest {

    private static final IceaxeTimeout CLOSE_TIMEOUT = new IceaxeTimeout(TgSessionOption.of(), TgTimeoutKey.DEFAULT);
    private static final IceaxeErrorCode CLOSE_TIMEOUT_ERROR = IceaxeErrorCode.SESSION_CLOSE_TIMEOUT;
    private static final IceaxeErrorCode CLOSE_ERROR = IceaxeErrorCode.SESSION_CHILD_CLOSE_ERROR;

    @Test
    void testGetAndCloseFuture() throws Exception {
        var count = new AtomicInteger(0);
        var future = new TestFutureResponse<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                assertEquals(123L, timeout);
                assertEquals(TimeUnit.MILLISECONDS, unit);
                return "abc";
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                count.addAndGet(1);
            }
        };
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.SESSION_CONNECT);
        var actual = IceaxeIoUtil.getAndCloseFuture(future, timeout, IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, IceaxeErrorCode.SESSION_CLOSE_TIMEOUT);
        assertEquals("abc", actual);
        assertEquals(1, count.get());
    }

    @Test
    void testGetAndCloseFuture_ServerException() throws Exception {
        var count = new AtomicInteger(0);
        var future = new TestFutureResponse<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new IceaxeServerExceptionTestMock("abc", 123);
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                count.addAndGet(1);
            }
        };
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(TsurugiIOException.class,
                () -> IceaxeIoUtil.getAndCloseFuture(future, timeout, IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, IceaxeErrorCode.SESSION_CLOSE_TIMEOUT));
        assertEquals("MOCK_123: abc", actual.getMessage());
        assertEquals("abc", actual.getCause().getMessage());
        assertEquals(1, count.get());
    }

    @Test
    void testGetAndCloseFuture_IOException() throws Exception {
        var count = new AtomicInteger(0);
        var future = new TestFutureResponse<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new IOException("abc");
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                count.addAndGet(1);
            }
        };
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(IOException.class, () -> IceaxeIoUtil.getAndCloseFuture(future, timeout, IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, IceaxeErrorCode.SESSION_CLOSE_TIMEOUT));
        assertEquals("abc", actual.getMessage());
        assertNull(actual.getCause());
        assertEquals(1, count.get());
    }

    @Test
    void testGetAndCloseFuture_InterruptedException() throws Exception {
        var count = new AtomicInteger(0);
        var future = new TestFutureResponse<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new InterruptedException("abc");
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                count.addAndGet(1);
            }
        };
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(InterruptedException.class,
                () -> IceaxeIoUtil.getAndCloseFuture(future, timeout, IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, IceaxeErrorCode.SESSION_CLOSE_TIMEOUT));
        assertEquals("abc", actual.getMessage());
        assertEquals(1, count.get());
    }

    @Test
    void testGetAndCloseFuture_timeout() throws Exception {
        var count = new AtomicInteger(0);
        var future = new TestFutureResponse<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new TimeoutException();
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                count.addAndGet(1);
            }
        };
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(IceaxeTimeoutIOException.class,
                () -> IceaxeIoUtil.getAndCloseFuture(future, timeout, IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, IceaxeErrorCode.SESSION_CLOSE_TIMEOUT));
        assertEquals(IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, actual.getDiagnosticCode());
        assertEquals(1, count.get());
    }

    @Test
    void testGetAndCloseFuture_closeServerException() throws Exception {
        var count = new AtomicInteger(0);
        var future = new TestFutureResponse<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                assertEquals(123L, timeout);
                assertEquals(TimeUnit.MILLISECONDS, unit);
                count.addAndGet(1);
                return "ignore";
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                throw new IceaxeServerExceptionTestMock("abc", 123);
            }
        };
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(TsurugiIOException.class,
                () -> IceaxeIoUtil.getAndCloseFuture(future, timeout, IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, IceaxeErrorCode.SESSION_CLOSE_TIMEOUT));
        assertEquals("MOCK_123: abc", actual.getMessage());
        assertEquals("abc", actual.getCause().getMessage());
    }

    @Test
    void testGetAndCloseFuture_closeIOException() throws Exception {
        var count = new AtomicInteger(0);
        var future = new TestFutureResponse<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                assertEquals(123L, timeout);
                assertEquals(TimeUnit.MILLISECONDS, unit);
                count.addAndGet(1);
                return "ignore";
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                throw new IOException("abc");
            }
        };
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(IOException.class, () -> IceaxeIoUtil.getAndCloseFuture(future, timeout, IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, IceaxeErrorCode.SESSION_CLOSE_TIMEOUT));
        assertEquals("abc", actual.getMessage());
        assertNull(actual.getCause());
    }

    @Test
    void testGetAndCloseFuture_closeInterruptedException() throws Exception {
        var count = new AtomicInteger(0);
        var future = new TestFutureResponse<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                assertEquals(123L, timeout);
                assertEquals(TimeUnit.MILLISECONDS, unit);
                count.addAndGet(1);
                return "ignore";
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                throw new InterruptedException("abc");
            }
        };
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(InterruptedException.class,
                () -> IceaxeIoUtil.getAndCloseFuture(future, timeout, IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, IceaxeErrorCode.SESSION_CLOSE_TIMEOUT));
        assertEquals("abc", actual.getMessage());
    }

    @Test
    void testGetAndCloseFuture_closeTimeout() throws Exception {
        var count = new AtomicInteger(0);
        var future = new TestFutureResponse<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                assertEquals(123L, timeout);
                assertEquals(TimeUnit.MILLISECONDS, unit);
                count.addAndGet(1);
                return "ignore";
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                throw new ResponseTimeoutException("abc");
            }
        };
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(IceaxeTimeoutIOException.class,
                () -> IceaxeIoUtil.getAndCloseFuture(future, timeout, IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, IceaxeErrorCode.SESSION_CLOSE_TIMEOUT));
        assertEquals(IceaxeErrorCode.SESSION_CLOSE_TIMEOUT, actual.getDiagnosticCode());
    }

    @Test
    void testGetAndCloseFuture_IOException_closeServerException() throws Exception {
        var future = new TestFutureResponse<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new IOException("abc");
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                throw new IceaxeServerExceptionTestMock("def", 456);
            }
        };
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(IOException.class, () -> IceaxeIoUtil.getAndCloseFuture(future, timeout, IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, IceaxeErrorCode.SESSION_CLOSE_TIMEOUT));
        assertEquals("abc", actual.getMessage());
        var s0 = actual.getSuppressed()[0];
        assertInstanceOf(TsurugiIOException.class, s0);
        assertEquals("MOCK_456: def", s0.getMessage());
        assertEquals("def", s0.getCause().getMessage());
    }

    @Test
    void testGetAndCloseFuture_IOException_closeIOException() throws Exception {
        var future = new TestFutureResponse<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new IOException("abc");
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                throw new IOException("def");
            }
        };
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(IOException.class, () -> IceaxeIoUtil.getAndCloseFuture(future, timeout, IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, IceaxeErrorCode.SESSION_CLOSE_TIMEOUT));
        assertEquals("abc", actual.getMessage());
        var s0 = actual.getSuppressed()[0];
        assertInstanceOf(IOException.class, s0);
        assertEquals("def", s0.getMessage());
    }

    @Test
    void testGetAndCloseFuture_IOException_closeInterruptedException() throws Exception {
        var future = new TestFutureResponse<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new IOException("abc");
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                throw new InterruptedException("def");
            }
        };
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(IOException.class, () -> IceaxeIoUtil.getAndCloseFuture(future, timeout, IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, IceaxeErrorCode.SESSION_CLOSE_TIMEOUT));
        assertEquals("abc", actual.getMessage());
        var s0 = actual.getSuppressed()[0];
        assertInstanceOf(InterruptedException.class, s0);
        assertEquals("def", s0.getMessage());
    }

    @Test
    void testGetAndCloseFutureInTransaction() throws Exception {
        var count = new AtomicInteger(0);
        var future = new TestFutureResponse<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                assertEquals(123L, timeout);
                assertEquals(TimeUnit.MILLISECONDS, unit);
                return "abc";
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                count.addAndGet(1);
            }
        };
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.SESSION_CONNECT);
        var actual = IceaxeIoUtil.getAndCloseFutureInTransaction(future, timeout, IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, IceaxeErrorCode.SESSION_CLOSE_TIMEOUT);
        assertEquals("abc", actual);
        assertEquals(1, count.get());
    }

    @Test
    void testGetAndCloseFutureInTransaction_ServerException() throws Exception {
        var count = new AtomicInteger(0);
        var future = new TestFutureResponse<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new IceaxeServerExceptionTestMock("abc", 123);
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                count.addAndGet(1);
            }
        };
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(TsurugiTransactionException.class,
                () -> IceaxeIoUtil.getAndCloseFutureInTransaction(future, timeout, IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, IceaxeErrorCode.SESSION_CLOSE_TIMEOUT));
        assertEquals("MOCK_123: abc", actual.getMessage());
        assertEquals("abc", actual.getCause().getMessage());
        assertEquals(1, count.get());
    }

    @Test
    void testGetAndCloseFutureInTransaction_timeout() throws Exception {
        var count = new AtomicInteger(0);
        var future = new TestFutureResponse<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new TimeoutException();
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                count.addAndGet(1);
            }
        };
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(IceaxeTimeoutIOException.class,
                () -> IceaxeIoUtil.getAndCloseFutureInTransaction(future, timeout, IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, IceaxeErrorCode.SESSION_CLOSE_TIMEOUT));
        assertEquals(IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, actual.getDiagnosticCode());
        assertInstanceOf(TimeoutException.class, actual.getCause());
        assertEquals(1, count.get());
    }

    @Test
    void testGetAndCloseFutureInTransaction_closeServerException() throws Exception {
        var count = new AtomicInteger(0);
        var future = new TestFutureResponse<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                assertEquals(123L, timeout);
                assertEquals(TimeUnit.MILLISECONDS, unit);
                count.addAndGet(1);
                return "ignore";
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                throw new IceaxeServerExceptionTestMock("abc", 123);
            }
        };
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(TsurugiTransactionException.class,
                () -> IceaxeIoUtil.getAndCloseFutureInTransaction(future, timeout, IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, IceaxeErrorCode.SESSION_CLOSE_TIMEOUT));
        assertEquals("MOCK_123: abc", actual.getMessage());
        assertEquals("abc", actual.getCause().getMessage());
    }

    @Test
    void testCloseable() throws Exception {
        var count = new AtomicInteger(0);
        var future = new TestFutureResponse<Void>() {
            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                count.addAndGet(1);
            }
        };
        try (var closeable = IceaxeIoUtil.closeable(future, CLOSE_TIMEOUT, CLOSE_TIMEOUT_ERROR)) {
        }
        assertEquals(1, count.get());
    }

    @Test
    void testCloseable_closeInterruptedException() throws Exception {
        var future = new TestFutureResponse<Void>() {
            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                throw new InterruptedException("abc");
            }
        };
        var e = assertThrowsExactly(InterruptedException.class, () -> {
            try (var closeable = IceaxeIoUtil.closeable(future, CLOSE_TIMEOUT, CLOSE_TIMEOUT_ERROR)) {
            }
        });
        assertEquals("abc", e.getMessage());
    }

    @Test
    void testCloseable_IOException_closeInterruptedException() throws Exception {
        var future = new TestFutureResponse<Void>() {
            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                throw new InterruptedException("def");
            }
        };
        var e = assertThrowsExactly(IOException.class, () -> {
            try (var closeable = IceaxeIoUtil.closeable(future, CLOSE_TIMEOUT, CLOSE_TIMEOUT_ERROR)) {
                throw new IOException("abc");
            }
        });
        assertEquals("abc", e.getMessage());
        var s0 = e.getSuppressed()[0];
        assertInstanceOf(InterruptedException.class, s0);
        assertEquals("def", s0.getMessage());
    }

    @Test
    void testCloseable_timeout() throws Exception {
        var future = new TestFutureResponse<Void>() {
            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                throw new ResponseTimeoutException("def");
            }
        };
        var e = assertThrowsExactly(Exception.class, () -> {
            try (var closeable = IceaxeIoUtil.closeable(future, CLOSE_TIMEOUT, CLOSE_TIMEOUT_ERROR)) {
                throw new Exception("abc");
            }
        });
        assertEquals("abc", e.getMessage());
        var s0 = (IceaxeIOException) e.getSuppressed()[0];
        assertEquals(CLOSE_TIMEOUT_ERROR, s0.getDiagnosticCode());
        assertEquals(CLOSE_TIMEOUT_ERROR.getMessage() + ": def", s0.getMessage());
    }

    @Test
    void testCloseRunnable() throws Exception {
        {
            var count = new AtomicInteger(0);
            var closeableSet = new IceaxeCloseableSet();
            IceaxeIoUtil.close(0, closeableSet, CLOSE_ERROR, t -> {
                count.addAndGet(1);
            });
            assertEquals(1, count.get());
        }
        {
            var count = new AtomicInteger(0);
            var closeableSet = new IceaxeCloseableSet();
            closeableSet.add(t -> count.addAndGet(1));
            IceaxeIoUtil.close(0, closeableSet, CLOSE_ERROR, t -> {
                count.addAndGet(1);
            });
            assertEquals(2, count.get());
        }
    }

    @Test
    void testCloseRunnable_ServerException() {
        {
            var closeableSet = new IceaxeCloseableSet();
            closeableSet.add(t -> {
                throw new IceaxeServerExceptionTestMock("abc", 123);
            });
            closeableSet.add(t -> {
                throw new IceaxeServerExceptionTestMock("def", 456);
            });

            var count = new AtomicInteger(0);
            var e = assertThrowsExactly(TsurugiIOException.class, () -> {
                IceaxeIoUtil.close(0, closeableSet, CLOSE_ERROR, t -> {
                    count.addAndGet(1);
                });
            });
            assertEquals(1, count.get());

            assertEquals("MOCK_123: abc", e.getMessage());
            var s0 = e.getSuppressed()[0];
            assertInstanceOf(TsurugiIOException.class, s0);
            assertEquals("MOCK_456: def", s0.getMessage());
        }
    }

    @Test
    void testCloseRunnable_IOException() {
        {
            var closeableSet = new IceaxeCloseableSet();
            closeableSet.add(t -> {
                throw new IOException("abc");
            });
            closeableSet.add(t -> {
                throw new IOException("def");
            });

            var count = new AtomicInteger(0);
            var e = assertThrowsExactly(IOException.class, () -> {
                IceaxeIoUtil.close(0, closeableSet, CLOSE_ERROR, t -> {
                    count.addAndGet(1);
                });
            });
            assertEquals(1, count.get());

            assertEquals("abc", e.getMessage());
            var s0 = e.getSuppressed()[0];
            assertInstanceOf(IOException.class, s0);
            assertEquals("def", s0.getMessage());
        }
    }

    @Test
    void testCloseRunnable_InterruptedException() {
        {
            var closeableSet = new IceaxeCloseableSet();
            closeableSet.add(t -> {
                throw new InterruptedException("abc");
            });
            closeableSet.add(t -> {
                throw new InterruptedException("def");
            });

            var count = new AtomicInteger(0);
            var e = assertThrowsExactly(IceaxeIOException.class, () -> {
                IceaxeIoUtil.close(0, closeableSet, CLOSE_ERROR, t -> {
                    count.addAndGet(1);
                });
            });
            assertEquals(1, count.get());

            assertEquals(CLOSE_ERROR, e.getDiagnosticCode());
            var cause = e.getCause();
            assertInstanceOf(InterruptedException.class, cause);
            assertEquals("abc", cause.getMessage());
            var s0 = e.getSuppressed()[0];
            assertInstanceOf(InterruptedException.class, s0);
            assertEquals("def", s0.getMessage());
        }
    }

    @Test
    void testCloseRunnable_ServerException_closeIOException() {
        {
            var closeableSet = new IceaxeCloseableSet();
            closeableSet.add(t -> {
                throw new IceaxeServerExceptionTestMock("def", 456);
            });

            var e = assertThrowsExactly(IOException.class, () -> {
                IceaxeIoUtil.close(0, closeableSet, CLOSE_ERROR, t -> {
                    throw new IOException("abc");
                });
            });

            assertEquals("abc", e.getMessage());
            var s0 = e.getSuppressed()[0];
            assertInstanceOf(TsurugiIOException.class, s0);
            assertEquals("MOCK_456: def", s0.getMessage());
        }
    }

    @Test
    void testCloseRunnable_IOException_closeIOException() {
        {
            var closeableSet = new IceaxeCloseableSet();
            closeableSet.add(t -> {
                throw new IOException("def");
            });

            var e = assertThrowsExactly(IOException.class, () -> {
                IceaxeIoUtil.close(0, closeableSet, CLOSE_ERROR, t -> {
                    throw new IOException("abc");
                });
            });

            assertEquals("abc", e.getMessage());
            var s0 = e.getSuppressed()[0];
            assertInstanceOf(IOException.class, s0);
            assertEquals("def", s0.getMessage());
        }
    }

    @Test
    void testClose() throws Exception {
        var count = new AtomicInteger();
        AutoCloseable close1 = () -> count.addAndGet(1);
        AutoCloseable close2 = () -> count.addAndGet(2);
        {
            count.set(0);
            IceaxeIoUtil.close(0, CLOSE_TIMEOUT_ERROR, CLOSE_ERROR, close1);
            assertEquals(1, count.get());
        }
        {
            count.set(0);
            IceaxeIoUtil.close(0, CLOSE_TIMEOUT_ERROR, CLOSE_ERROR, close1, close2);
            assertEquals(3, count.get());
        }
        {
            count.set(0);
            IceaxeIoUtil.close(0, CLOSE_TIMEOUT_ERROR, CLOSE_ERROR, close1, null, close2);
            assertEquals(3, count.get());
        }
    }

    @Test
    void testClose_ServerException() {
        AutoCloseable close1 = () -> {
            throw new IceaxeServerExceptionTestMock("abc", 123);
        };
        AutoCloseable close2 = () -> {
            throw new IceaxeServerExceptionTestMock("def", 456);
        };
        var e = assertThrowsExactly(TsurugiIOException.class, () -> {
            IceaxeIoUtil.close(0, CLOSE_TIMEOUT_ERROR, CLOSE_ERROR, close1, close2);
        });
        assertEquals("MOCK_123: abc", e.getMessage());
        var s0 = e.getSuppressed()[0];
        assertInstanceOf(TsurugiIOException.class, s0);
        assertEquals("MOCK_456: def", s0.getMessage());
    }

    @Test
    void testClose_IceaxeIOException() {
        { // e1 is timeout, e1.code != close.timeoutCode
            var e1 = new IceaxeTimeoutIOException(IceaxeErrorCode.SESSION_SHUTDOWN_CLOSE_TIMEOUT);
            var e2 = new IceaxeIOException(CLOSE_ERROR);
            AutoCloseable close1 = () -> {
                throw e1;
            };
            AutoCloseable close2 = () -> {
                throw e2;
            };
            var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> {
                IceaxeIoUtil.close(0, CLOSE_TIMEOUT_ERROR, CLOSE_ERROR, close1, close2);
            });
            assertEquals(CLOSE_TIMEOUT_ERROR, e.getDiagnosticCode());
            assertSame(e1, e.getCause());
            var s0 = e.getSuppressed()[0];
            assertSame(e2, s0);
        }
        { // e1 is timeout, e1.code == close.timeoutCode
            var e1 = new IceaxeTimeoutIOException(CLOSE_TIMEOUT_ERROR);
            var e2 = new IceaxeIOException(CLOSE_ERROR);
            AutoCloseable close1 = () -> {
                throw e1;
            };
            AutoCloseable close2 = () -> {
                throw e2;
            };
            var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> {
                IceaxeIoUtil.close(0, CLOSE_TIMEOUT_ERROR, CLOSE_ERROR, close1, close2);
            });
            assertSame(e1, e);
            var s0 = e.getSuppressed()[0];
            assertSame(e2, s0);
        }
        { // e1 is not timeout, e1.code != close.errorCode
            var e1 = new IceaxeIOException(IceaxeErrorCode.SESSION_LOW_ERROR);
            var e2 = new IceaxeIOException(CLOSE_ERROR);
            AutoCloseable close1 = () -> {
                throw e1;
            };
            AutoCloseable close2 = () -> {
                throw e2;
            };
            var e = assertThrowsExactly(IceaxeIOException.class, () -> {
                IceaxeIoUtil.close(0, CLOSE_TIMEOUT_ERROR, CLOSE_ERROR, close1, close2);
            });
            assertEquals(CLOSE_ERROR, e.getDiagnosticCode());
            assertSame(e1, e.getCause());
            var s0 = e.getSuppressed()[0];
            assertSame(e2, s0);
        }
        { // e1 is not timeout, e1.code == close.errorCode
            var e1 = new IceaxeIOException(CLOSE_ERROR);
            var e2 = new IceaxeIOException(IceaxeErrorCode.SESSION_CHILD_CLOSE_ERROR);
            AutoCloseable close1 = () -> {
                throw e1;
            };
            AutoCloseable close2 = () -> {
                throw e2;
            };
            var e = assertThrowsExactly(IceaxeIOException.class, () -> {
                IceaxeIoUtil.close(0, CLOSE_TIMEOUT_ERROR, CLOSE_ERROR, close1, close2);
            });
            assertSame(e1, e);
            var s0 = e.getSuppressed()[0];
            assertSame(e2, s0);
        }
    }

    @Test
    void testClose_IOException() {
        AutoCloseable close1 = () -> {
            throw new IOException("abc");
        };
        AutoCloseable close2 = () -> {
            throw new IOException("def");
        };
        var e = assertThrowsExactly(IOException.class, () -> {
            IceaxeIoUtil.close(0, CLOSE_TIMEOUT_ERROR, CLOSE_ERROR, close1, close2);
        });
        assertEquals("abc", e.getMessage());
        var s0 = e.getSuppressed()[0];
        assertInstanceOf(IOException.class, s0);
        assertEquals("def", s0.getMessage());
    }

    @Test
    void testClose_InterruptedException2() {
        AutoCloseable close1 = () -> {
            throw new InterruptedException("abc");
        };
        AutoCloseable close2 = () -> {
            throw new InterruptedException("def");
        };
        var e = assertThrowsExactly(InterruptedException.class, () -> {
            IceaxeIoUtil.close(0, CLOSE_TIMEOUT_ERROR, CLOSE_ERROR, close1, close2);
        });
        assertEquals("abc", e.getMessage());
        var s0 = e.getSuppressed()[0];
        assertInstanceOf(InterruptedException.class, s0);
        assertEquals("def", s0.getMessage());
    }

    @Test
    void testClose_RuntimeException() {
        AutoCloseable close1 = () -> {
            throw new RuntimeException("abc");
        };
        AutoCloseable close2 = () -> {
            throw new RuntimeException("def");
        };
        var e = assertThrowsExactly(RuntimeException.class, () -> {
            IceaxeIoUtil.close(0, CLOSE_TIMEOUT_ERROR, CLOSE_ERROR, close1, close2);
        });
        assertEquals("abc", e.getMessage());
        var s0 = e.getSuppressed()[0];
        assertInstanceOf(RuntimeException.class, s0);
        assertEquals("def", s0.getMessage());
    }

    @Test
    void testClose_Error() {
        AutoCloseable close1 = () -> {
            throw new Error("abc");
        };
        AutoCloseable close2 = () -> {
            throw new Error("def");
        };
        var e = assertThrowsExactly(Error.class, () -> {
            IceaxeIoUtil.close(0, CLOSE_TIMEOUT_ERROR, CLOSE_ERROR, close1, close2);
        });
        assertEquals("abc", e.getMessage());
        var s0 = e.getSuppressed()[0];
        assertInstanceOf(Error.class, s0);
        assertEquals("def", s0.getMessage());
    }

    @Test
    void testClose_timeout() {
        AutoCloseable close1 = () -> {
            throw new ResponseTimeoutException("abc");
        };
        AutoCloseable close2 = () -> {
            throw new ResponseTimeoutException("def");
        };
        var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> {
            IceaxeIoUtil.close(0, CLOSE_TIMEOUT_ERROR, CLOSE_ERROR, close1, close2);
        });
        assertEquals(CLOSE_TIMEOUT_ERROR.getMessage() + ": abc", e.getMessage());
        var s0 = e.getSuppressed()[0];
        assertInstanceOf(IceaxeIOException.class, s0);
        assertEquals(CLOSE_TIMEOUT_ERROR.getMessage() + ": def", s0.getMessage());
    }

    @Test
    void testCloseInTransaction() throws Exception {
        var count = new AtomicInteger();
        AutoCloseable close1 = () -> count.addAndGet(1);
        AutoCloseable close2 = () -> count.addAndGet(2);
        {
            count.set(0);
            IceaxeIoUtil.closeInTransaction(0, CLOSE_TIMEOUT_ERROR, CLOSE_ERROR, close1);
            assertEquals(1, count.get());
        }
        {
            count.set(0);
            IceaxeIoUtil.closeInTransaction(0, CLOSE_TIMEOUT_ERROR, CLOSE_ERROR, close1, close2);
            assertEquals(3, count.get());
        }
        {
            count.set(0);
            IceaxeIoUtil.closeInTransaction(0, CLOSE_TIMEOUT_ERROR, CLOSE_ERROR, close1, null, close2);
            assertEquals(3, count.get());
        }
    }

    @Test
    void testCloseInTransaction_ServerException() {
        AutoCloseable close1 = () -> {
            throw new IceaxeServerExceptionTestMock("abc", 123);
        };
        AutoCloseable close2 = () -> {
            throw new IceaxeServerExceptionTestMock("def", 456);
        };
        var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
            IceaxeIoUtil.closeInTransaction(0, CLOSE_TIMEOUT_ERROR, CLOSE_ERROR, close1, close2);
        });
        assertEquals("MOCK_123: abc", e.getMessage());
        var s0 = e.getSuppressed()[0];
        assertInstanceOf(TsurugiTransactionException.class, s0);
        assertEquals("MOCK_456: def", s0.getMessage());
    }

    @Test
    void testCloseInTransaction_timeout() {
        AutoCloseable close1 = () -> {
            throw new ResponseTimeoutException("abc");
        };
        AutoCloseable close2 = () -> {
            throw new ResponseTimeoutException("def");
        };
        var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> {
            IceaxeIoUtil.closeInTransaction(0, CLOSE_TIMEOUT_ERROR, CLOSE_ERROR, close1, close2);
        });
        assertEquals(CLOSE_TIMEOUT_ERROR.getMessage() + ": abc", e.getMessage());
        var s0 = e.getSuppressed()[0];
        assertInstanceOf(IceaxeIOException.class, s0);
        assertEquals(CLOSE_TIMEOUT_ERROR.getMessage() + ": def", s0.getMessage());
    }

    @Test
    void calculateTimeoutNanos() throws InterruptedException {
        long totalTimeout = TimeUnit.MILLISECONDS.toNanos(300);
        long start = System.nanoTime();

        {
            long sleepNanos = TimeUnit.MILLISECONDS.toNanos(50);
            TimeUnit.NANOSECONDS.sleep(sleepNanos * 2);

            long timeout = IceaxeIoUtil.calculateTimeoutNanos(totalTimeout, start);
            long expected = totalTimeout - sleepNanos;
            if (timeout < expected) {
                // success
            } else {
                fail(String.format("fail. timeout=%d, totalTimeout=%d, expected=%d", timeout, totalTimeout, expected));
            }
        }

        { // minimum
            TimeUnit.NANOSECONDS.sleep(totalTimeout);
            long timeout = IceaxeIoUtil.calculateTimeoutNanos(totalTimeout, start);
            assertEquals(TimeUnit.MILLISECONDS.toNanos(1), timeout);
        }
    }
}
