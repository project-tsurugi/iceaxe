package com.tsurugidb.iceaxe.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeServerExceptionTestMock;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.tsubakuro.exception.ServerException;

class IceaxeIoUtilTest {

    @Test
    void testGetAndCloseFuture() throws IOException {
        var count = new AtomicInteger(0);
        var future = new IceaxeFutureResponseTestMock<String>() {
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
        var info = TgSessionInfo.of().timeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.SESSION_CONNECT);
        var actual = IceaxeIoUtil.getAndCloseFuture(future, timeout);
        assertEquals("abc", actual);
        assertEquals(1, count.get());
    }

    @Test
    void testGetAndCloseFutureEx() throws IOException {
        var count = new AtomicInteger(0);
        var future = new IceaxeFutureResponseTestMock<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new IceaxeServerExceptionTestMock("abc", 123);
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                count.addAndGet(1);
            }
        };
        var info = TgSessionInfo.of().timeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(TsurugiIOException.class, () -> IceaxeIoUtil.getAndCloseFuture(future, timeout));
        assertEquals("MOCK_123: abc", actual.getMessage());
        assertEquals("abc", actual.getCause().getMessage());
        assertEquals(1, count.get());
    }

    @Test
    void testGetAndCloseFutureIOEx() throws IOException {
        var count = new AtomicInteger(0);
        var future = new IceaxeFutureResponseTestMock<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new IOException("abc");
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                count.addAndGet(1);
            }
        };
        var info = TgSessionInfo.of().timeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(IOException.class, () -> IceaxeIoUtil.getAndCloseFuture(future, timeout));
        assertEquals("abc", actual.getMessage());
        assertNull(actual.getCause());
        assertEquals(1, count.get());
    }

    @Test
    void testGetAndCloseFutureIOEx2() throws IOException {
        var count = new AtomicInteger(0);
        var future = new IceaxeFutureResponseTestMock<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new InterruptedException("abc");
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                count.addAndGet(1);
            }
        };
        var info = TgSessionInfo.of().timeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(IOException.class, () -> IceaxeIoUtil.getAndCloseFuture(future, timeout));
        assertEquals("abc", actual.getMessage());
        assertEquals("abc", actual.getCause().getMessage());
        assertEquals(1, count.get());
    }

    @Test
    void testGetAndCloseFutureCloseEx() throws IOException {
        var count = new AtomicInteger(0);
        var future = new IceaxeFutureResponseTestMock<String>() {
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
        var info = TgSessionInfo.of().timeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(TsurugiIOException.class, () -> IceaxeIoUtil.getAndCloseFuture(future, timeout));
        assertEquals("MOCK_123: abc", actual.getMessage());
        assertEquals("abc", actual.getCause().getMessage());
    }

    @Test
    void testGetAndCloseFutureCloseIOEx() throws IOException {
        var count = new AtomicInteger(0);
        var future = new IceaxeFutureResponseTestMock<String>() {
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
        var info = TgSessionInfo.of().timeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(IOException.class, () -> IceaxeIoUtil.getAndCloseFuture(future, timeout));
        assertEquals("abc", actual.getMessage());
        assertNull(actual.getCause());
    }

    @Test
    void testGetAndCloseFutureCloseIOEx2() throws IOException {
        var count = new AtomicInteger(0);
        var future = new IceaxeFutureResponseTestMock<String>() {
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
        var info = TgSessionInfo.of().timeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(IOException.class, () -> IceaxeIoUtil.getAndCloseFuture(future, timeout));
        assertEquals("abc", actual.getMessage());
        assertEquals("abc", actual.getCause().getMessage());
    }

    @Test
    void testGetAndCloseFutureExCloseEx() throws IOException {
        var future = new IceaxeFutureResponseTestMock<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new IOException("abc");
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                throw new IceaxeServerExceptionTestMock("def", 456);
            }
        };
        var info = TgSessionInfo.of().timeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(IOException.class, () -> IceaxeIoUtil.getAndCloseFuture(future, timeout));
        assertEquals("abc", actual.getMessage());
        var s0 = actual.getSuppressed()[0];
        assertInstanceOf(TsurugiIOException.class, s0);
        assertEquals("MOCK_456: def", s0.getMessage());
        assertEquals("def", s0.getCause().getMessage());
    }

    @Test
    void testGetAndCloseFutureExCloseIOEx() throws IOException {
        var future = new IceaxeFutureResponseTestMock<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new IOException("abc");
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                throw new IOException("def");
            }
        };
        var info = TgSessionInfo.of().timeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(IOException.class, () -> IceaxeIoUtil.getAndCloseFuture(future, timeout));
        assertEquals("abc", actual.getMessage());
        var s0 = actual.getSuppressed()[0];
        assertInstanceOf(IOException.class, s0);
        assertEquals("def", s0.getMessage());
    }

    @Test
    void testGetAndCloseFutureExCloseIOEx2() throws IOException {
        var future = new IceaxeFutureResponseTestMock<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new IOException("abc");
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                throw new InterruptedException("def");
            }
        };
        var info = TgSessionInfo.of().timeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(IOException.class, () -> IceaxeIoUtil.getAndCloseFuture(future, timeout));
        assertEquals("abc", actual.getMessage());
        var s0 = actual.getSuppressed()[0];
        assertInstanceOf(InterruptedException.class, s0);
        assertEquals("def", s0.getMessage());
    }

    @Test
    void testGetAndCloseFutureInTransaction() throws IOException, TsurugiTransactionException {
        var count = new AtomicInteger(0);
        var future = new IceaxeFutureResponseTestMock<String>() {
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
        var info = TgSessionInfo.of().timeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.SESSION_CONNECT);
        var actual = IceaxeIoUtil.getAndCloseFutureInTransaction(future, timeout);
        assertEquals("abc", actual);
        assertEquals(1, count.get());
    }

    @Test
    void testGetAndCloseFutureInTransactionEx() throws IOException {
        var count = new AtomicInteger(0);
        var future = new IceaxeFutureResponseTestMock<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new IceaxeServerExceptionTestMock("abc", 123);
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                count.addAndGet(1);
            }
        };
        var info = TgSessionInfo.of().timeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(TsurugiTransactionException.class, () -> IceaxeIoUtil.getAndCloseFutureInTransaction(future, timeout));
        assertEquals("MOCK_123: abc", actual.getMessage());
        assertEquals("abc", actual.getCause().getMessage());
        assertEquals(1, count.get());
    }

    @Test
    void testGetAndCloseFutureCloseInTransactionEx() throws IOException {
        var count = new AtomicInteger(0);
        var future = new IceaxeFutureResponseTestMock<String>() {
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
        var info = TgSessionInfo.of().timeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrowsExactly(TsurugiTransactionException.class, () -> IceaxeIoUtil.getAndCloseFutureInTransaction(future, timeout));
        assertEquals("MOCK_123: abc", actual.getMessage());
        assertEquals("abc", actual.getCause().getMessage());
    }

    @Test
    void testCloseable() throws IOException {
        var count = new AtomicInteger(0);
        var future = new IceaxeFutureResponseTestMock<Void>() {
            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                count.addAndGet(1);
            }
        };
        try (var closeable = IceaxeIoUtil.closeable(future)) {
        }
        assertEquals(1, count.get());
    }

    @Test
    void testCloseableIOEx() throws IOException {
        var future = new IceaxeFutureResponseTestMock<Void>() {
            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                throw new InterruptedException("abc");
            }
        };
        var e = assertThrowsExactly(IOException.class, () -> {
            try (var closeable = IceaxeIoUtil.closeable(future)) {
            }
        });
        assertEquals("abc", e.getMessage());
        var c = e.getCause();
        assertInstanceOf(InterruptedException.class, c);
    }

    @Test
    void testCloseableIOEx2() throws IOException {
        var future = new IceaxeFutureResponseTestMock<Void>() {
            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                throw new InterruptedException("def");
            }
        };
        var e = assertThrowsExactly(IOException.class, () -> {
            try (var closeable = IceaxeIoUtil.closeable(future)) {
                throw new IOException("abc");
            }
        });
        assertEquals("abc", e.getMessage());
        var s0 = e.getSuppressed()[0];
        assertInstanceOf(IOException.class, s0);
        var c = s0.getCause();
        assertInstanceOf(InterruptedException.class, c);
        assertEquals("def", c.getMessage());
    }

    @Test
    void testCloseRunnable() throws IOException {
        {
            var count = new AtomicInteger(0);
            var closeableSet = new IceaxeCloseableSet();
            IceaxeIoUtil.close(closeableSet, () -> {
                count.addAndGet(1);
            });
            assertEquals(1, count.get());
        }
        {
            var count = new AtomicInteger(0);
            var closeableSet = new IceaxeCloseableSet();
            closeableSet.add(() -> count.addAndGet(1));
            IceaxeIoUtil.close(closeableSet, () -> {
                count.addAndGet(1);
            });
            assertEquals(2, count.get());
        }
    }

    @Test
    void testCloseRunnableEx() {
        {
            var closeableSet = new IceaxeCloseableSet();
            closeableSet.add(() -> {
                throw new IceaxeServerExceptionTestMock("abc", 123);
            });
            closeableSet.add(() -> {
                throw new IceaxeServerExceptionTestMock("def", 456);
            });

            var count = new AtomicInteger(0);
            var e = assertThrowsExactly(TsurugiIOException.class, () -> IceaxeIoUtil.close(closeableSet, () -> {
                count.addAndGet(1);
            }));
            assertEquals(1, count.get());

            assertEquals("MOCK_123: abc", e.getMessage());
            var s0 = e.getSuppressed()[0];
            assertInstanceOf(TsurugiIOException.class, s0);
            assertEquals("MOCK_456: def", s0.getMessage());
        }
    }

    @Test
    void testCloseRunnableIOEx() {
        {
            var closeableSet = new IceaxeCloseableSet();
            closeableSet.add(() -> {
                throw new IOException("abc");
            });
            closeableSet.add(() -> {
                throw new IOException("def");
            });

            var count = new AtomicInteger(0);
            var e = assertThrowsExactly(IOException.class, () -> IceaxeIoUtil.close(closeableSet, () -> {
                count.addAndGet(1);
            }));
            assertEquals(1, count.get());

            assertEquals("abc", e.getMessage());
            var s0 = e.getSuppressed()[0];
            assertInstanceOf(IOException.class, s0);
            assertEquals("def", s0.getMessage());
        }
    }

    @Test
    void testCloseRunnableIOEx2() {
        {
            var closeableSet = new IceaxeCloseableSet();
            closeableSet.add(() -> {
                throw new InterruptedException("abc");
            });
            closeableSet.add(() -> {
                throw new InterruptedException("def");
            });

            var count = new AtomicInteger(0);
            var e = assertThrowsExactly(IOException.class, () -> IceaxeIoUtil.close(closeableSet, () -> {
                count.addAndGet(1);
            }));
            assertEquals(1, count.get());

            assertEquals("abc", e.getMessage());
            var s0 = e.getSuppressed()[0];
            assertInstanceOf(InterruptedException.class, s0);
            assertEquals("def", s0.getMessage());
        }
    }

    @Test
    void testCloseRunnableCloseEx() {
        {
            var closeableSet = new IceaxeCloseableSet();
            closeableSet.add(() -> {
                throw new IceaxeServerExceptionTestMock("def", 456);
            });

            var e = assertThrowsExactly(IOException.class, () -> IceaxeIoUtil.close(closeableSet, () -> {
                throw new IOException("abc");
            }));

            assertEquals("abc", e.getMessage());
            var s0 = e.getSuppressed()[0];
            assertInstanceOf(TsurugiIOException.class, s0);
            assertEquals("MOCK_456: def", s0.getMessage());
        }
    }

    @Test
    void testCloseRunnableCloseIOEx() {
        {
            var closeableSet = new IceaxeCloseableSet();
            closeableSet.add(() -> {
                throw new IOException("def");
            });

            var e = assertThrowsExactly(IOException.class, () -> IceaxeIoUtil.close(closeableSet, () -> {
                throw new IOException("abc");
            }));

            assertEquals("abc", e.getMessage());
            var s0 = e.getSuppressed()[0];
            assertInstanceOf(IOException.class, s0);
            assertEquals("def", s0.getMessage());
        }
    }

    @Test
    void testClose() throws IOException {
        var count = new AtomicInteger();
        AutoCloseable close1 = () -> count.addAndGet(1);
        AutoCloseable close2 = () -> count.addAndGet(2);
        {
            count.set(0);
            IceaxeIoUtil.close(close1);
            assertEquals(1, count.get());
        }
        {
            count.set(0);
            IceaxeIoUtil.close(close1, close2);
            assertEquals(3, count.get());
        }
        {
            count.set(0);
            IceaxeIoUtil.close(close1, null, close2);
            assertEquals(3, count.get());
        }
    }

    @Test
    void testCloseEx() {
        AutoCloseable close1 = () -> {
            throw new IceaxeServerExceptionTestMock("abc", 123);
        };
        AutoCloseable close2 = () -> {
            throw new IceaxeServerExceptionTestMock("def", 456);
        };
        var e = assertThrowsExactly(TsurugiIOException.class, () -> IceaxeIoUtil.close(close1, close2));
        assertEquals("MOCK_123: abc", e.getMessage());
        var s0 = e.getSuppressed()[0];
        assertInstanceOf(TsurugiIOException.class, s0);
        assertEquals("MOCK_456: def", s0.getMessage());
    }

    @Test
    void testCloseIOEx() {
        AutoCloseable close1 = () -> {
            throw new IOException("abc");
        };
        AutoCloseable close2 = () -> {
            throw new IOException("def");
        };
        var e = assertThrowsExactly(IOException.class, () -> IceaxeIoUtil.close(close1, close2));
        assertEquals("abc", e.getMessage());
        var s0 = e.getSuppressed()[0];
        assertInstanceOf(IOException.class, s0);
        assertEquals("def", s0.getMessage());
    }

    @Test
    void testCloseIOEx2() {
        AutoCloseable close1 = () -> {
            throw new InterruptedException("abc");
        };
        AutoCloseable close2 = () -> {
            throw new InterruptedException("def");
        };
        var e = assertThrowsExactly(IOException.class, () -> IceaxeIoUtil.close(close1, close2));
        assertEquals("abc", e.getMessage());
        assertInstanceOf(InterruptedException.class, e.getCause());
        var s0 = e.getSuppressed()[0];
        assertInstanceOf(InterruptedException.class, s0);
        assertEquals("def", s0.getMessage());
    }

    @Test
    void testCloseREx() {
        AutoCloseable close1 = () -> {
            throw new RuntimeException("abc");
        };
        AutoCloseable close2 = () -> {
            throw new RuntimeException("def");
        };
        var e = assertThrowsExactly(RuntimeException.class, () -> IceaxeIoUtil.close(close1, close2));
        assertEquals("abc", e.getMessage());
        var s0 = e.getSuppressed()[0];
        assertInstanceOf(RuntimeException.class, s0);
        assertEquals("def", s0.getMessage());
    }

    @Test
    void testCloseErr() {
        AutoCloseable close1 = () -> {
            throw new Error("abc");
        };
        AutoCloseable close2 = () -> {
            throw new Error("def");
        };
        var e = assertThrowsExactly(Error.class, () -> IceaxeIoUtil.close(close1, close2));
        assertEquals("abc", e.getMessage());
        var s0 = e.getSuppressed()[0];
        assertInstanceOf(Error.class, s0);
        assertEquals("def", s0.getMessage());
    }

    @Test
    void testCloseInTransaction() throws IOException, TsurugiTransactionException {
        var count = new AtomicInteger();
        AutoCloseable close1 = () -> count.addAndGet(1);
        AutoCloseable close2 = () -> count.addAndGet(2);
        {
            count.set(0);
            IceaxeIoUtil.closeInTransaction(close1);
            assertEquals(1, count.get());
        }
        {
            count.set(0);
            IceaxeIoUtil.closeInTransaction(close1, close2);
            assertEquals(3, count.get());
        }
        {
            count.set(0);
            IceaxeIoUtil.closeInTransaction(close1, null, close2);
            assertEquals(3, count.get());
        }
    }

    @Test
    void testCloseInTransactionEx() {
        AutoCloseable close1 = () -> {
            throw new IceaxeServerExceptionTestMock("abc", 123);
        };
        AutoCloseable close2 = () -> {
            throw new IceaxeServerExceptionTestMock("def", 456);
        };
        var e = assertThrowsExactly(TsurugiTransactionException.class, () -> IceaxeIoUtil.closeInTransaction(close1, close2));
        assertEquals("MOCK_123: abc", e.getMessage());
        var s0 = e.getSuppressed()[0];
        assertInstanceOf(TsurugiTransactionException.class, s0);
        assertEquals("MOCK_456: def", s0.getMessage());
    }
}
