package com.tsurugidb.iceaxe.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.exception.DiagnosticCode;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.exception.SqlServiceCode;
import com.nautilus_technologies.tsubakuro.util.Timeout;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionException;

class IceaxeIoUtilTest {

    @SuppressWarnings("serial")
    static class IceaxeServerExceptionTestMock extends ServerException {

        public IceaxeServerExceptionTestMock(String message) {
            super(message);
        }

        @Override
        public DiagnosticCode getDiagnosticCode() {
            return SqlServiceCode.OK;
        }
    }

    @Test
    void testGetFromFuture() throws IOException {
        var future = new IceaxeFutureResponseTestMock<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                assertEquals(123L, timeout);
                assertEquals(TimeUnit.MILLISECONDS, unit);
                return "abc";
            }
        };
        var info = TgSessionInfo.of().timeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.SESSION_CONNECT);
        var actual = IceaxeIoUtil.getFromFuture(future, timeout);
        assertEquals("abc", actual);
    }

    @Test
    void testGetFromFutureIOEx() {
        var future = new IceaxeFutureResponseTestMock<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new IceaxeServerExceptionTestMock("abc");
            }
        };
        var info = TgSessionInfo.of();
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrows(IOException.class, () -> IceaxeIoUtil.getFromFuture(future, timeout));
        assertEquals("abc", actual.getCause().getMessage());
    }

    @Test
    void testGetFromTransactionFuture() throws IOException, TsurugiTransactionException {
        var future = new IceaxeFutureResponseTestMock<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                assertEquals(123L, timeout);
                assertEquals(TimeUnit.MILLISECONDS, unit);
                return "abc";
            }
        };
        var info = TgSessionInfo.of().timeout(TgTimeoutKey.TRANSACTION_COMMIT, 123, TimeUnit.MILLISECONDS);
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.TRANSACTION_COMMIT);
        var actual = IceaxeIoUtil.getFromTransactionFuture(future, timeout);
        assertEquals("abc", actual);
    }

    @Test
    void testGetFromTransactionFutureEx() {
        var future = new IceaxeFutureResponseTestMock<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new IceaxeServerExceptionTestMock("abc");
            }
        };
        var info = TgSessionInfo.of();
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.TRANSACTION_COMMIT);
        var actual = assertThrows(TsurugiTransactionException.class, () -> IceaxeIoUtil.getFromTransactionFuture(future, timeout));
        assertEquals("abc", actual.getCause().getMessage());
    }

    @Test
    void testGetFromTransactionFutureIOEx() {
        var future = new IceaxeFutureResponseTestMock<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new TimeoutException("abc");
            }
        };
        var info = TgSessionInfo.of();
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.TRANSACTION_COMMIT);
        var actual = assertThrows(IOException.class, () -> IceaxeIoUtil.getFromTransactionFuture(future, timeout));
        assertEquals("abc", actual.getCause().getMessage());
    }

    @Test
    void testCheckAndCloseTransactionFuture() throws IOException, TsurugiTransactionException {
        var future = new IceaxeFutureResponseTestMock<Void>() {
            @Override
            public Void get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                assertEquals(123L, timeout);
                assertEquals(TimeUnit.MILLISECONDS, unit);
                return null;
            }

            @Override
            public void close() throws IOException, ServerException, InterruptedException {
                assertLowTimeout(456, TimeUnit.SECONDS, closeTimeout);
            }
        };
        var info = TgSessionInfo.of().timeout(TgTimeoutKey.TRANSACTION_COMMIT, 123, TimeUnit.MILLISECONDS).timeout(TgTimeoutKey.TRANSACTION_CLOSE, 456, TimeUnit.SECONDS);
        var checkTimeout = new IceaxeTimeout(info, TgTimeoutKey.TRANSACTION_COMMIT);
        var closeTimeout = new IceaxeTimeout(info, TgTimeoutKey.TRANSACTION_CLOSE);
        IceaxeIoUtil.checkAndCloseTransactionFuture(future, checkTimeout, closeTimeout);
    }

    @Test
    void testCheckAndCloseTransactionFutureEx() {
        var info = TgSessionInfo.of().timeout(TgTimeoutKey.TRANSACTION_COMMIT, 123, TimeUnit.MILLISECONDS).timeout(TgTimeoutKey.TRANSACTION_CLOSE, 456, TimeUnit.SECONDS);
        var checkTimeout = new IceaxeTimeout(info, TgTimeoutKey.TRANSACTION_COMMIT);
        var closeTimeout = new IceaxeTimeout(info, TgTimeoutKey.TRANSACTION_CLOSE);
        {
            var future = new IceaxeFutureResponseTestMock<Void>() {
                @Override
                public Void get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                    throw new IceaxeServerExceptionTestMock("abc");
                }
            };
            var actual = assertThrows(TsurugiTransactionException.class, () -> IceaxeIoUtil.checkAndCloseTransactionFuture(future, checkTimeout, closeTimeout));
            assertEquals("abc", actual.getCause().getMessage());
        }
        {
            var future = new IceaxeFutureResponseTestMock<Void>() {
                @Override
                public Void get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                    assertEquals(123L, timeout);
                    assertEquals(TimeUnit.MILLISECONDS, unit);
                    return null;
                }

                @Override
                public void close() throws IOException, ServerException, InterruptedException {
                    throw new IceaxeServerExceptionTestMock("abc");
                }
            };
            var actual = assertThrows(TsurugiTransactionException.class, () -> IceaxeIoUtil.checkAndCloseTransactionFuture(future, checkTimeout, closeTimeout));
            assertEquals("abc", actual.getCause().getMessage());
        }
    }

    @Test
    void testCheckAndCloseTransactionFutureIOEx() {
        var info = TgSessionInfo.of().timeout(TgTimeoutKey.TRANSACTION_COMMIT, 123, TimeUnit.MILLISECONDS).timeout(TgTimeoutKey.TRANSACTION_CLOSE, 456, TimeUnit.SECONDS);
        var checkTimeout = new IceaxeTimeout(info, TgTimeoutKey.TRANSACTION_COMMIT);
        var closeTimeout = new IceaxeTimeout(info, TgTimeoutKey.TRANSACTION_CLOSE);
        {
            var future = new IceaxeFutureResponseTestMock<Void>() {
                @Override
                public Void get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                    throw new TimeoutException("abc");
                }
            };
            var actual = assertThrows(IOException.class, () -> IceaxeIoUtil.checkAndCloseTransactionFuture(future, checkTimeout, closeTimeout));
            assertEquals("abc", actual.getCause().getMessage());
        }
        {
            var future = new IceaxeFutureResponseTestMock<Void>() {
                @Override
                public Void get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                    assertEquals(123L, timeout);
                    assertEquals(TimeUnit.MILLISECONDS, unit);
                    return null;
                }

                @Override
                public void close() throws IOException, ServerException, InterruptedException {
                    throw new InterruptedException("abc");
                }
            };
            var actual = assertThrows(IOException.class, () -> IceaxeIoUtil.checkAndCloseTransactionFuture(future, checkTimeout, closeTimeout));
            assertEquals("abc", actual.getCause().getMessage());
        }
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
            var count = new AtomicInteger(0);
            var closeableSet = new IceaxeCloseableSet();
            var e = assertThrows(IOException.class, () -> IceaxeIoUtil.close(closeableSet, () -> {
                count.addAndGet(1);
                throw new IOException("abc");
            }));
            assertEquals(1, count.get());

            assertEquals("abc", e.getMessage());
        }

        {
            var count = new AtomicInteger(0);
            var closeableSet = new IceaxeCloseableSet();
            closeableSet.add(() -> {
                count.addAndGet(1);
                throw new IOException("abc");
            });
            var e = assertThrows(IOException.class, () -> IceaxeIoUtil.close(closeableSet, () -> {
                count.addAndGet(1);
            }));
            assertEquals(2, count.get());

            assertEquals("abc", e.getMessage());
        }
        {
            var count = new AtomicInteger(0);
            var closeableSet = new IceaxeCloseableSet();
            closeableSet.add(() -> count.addAndGet(1));
            var e = assertThrows(IOException.class, () -> IceaxeIoUtil.close(closeableSet, () -> {
                count.addAndGet(1);
                throw new IOException("abc");
            }));
            assertEquals(2, count.get());

            assertEquals("abc", e.getMessage());
        }

        {
            var count = new AtomicInteger(0);
            var closeableSet = new IceaxeCloseableSet();
            closeableSet.add(() -> {
                count.addAndGet(1);
                throw new IOException("def");
            });
            var e = assertThrows(IOException.class, () -> IceaxeIoUtil.close(closeableSet, () -> {
                count.addAndGet(1);
                throw new IOException("abc");
            }));
            assertEquals(2, count.get());

            assertEquals("abc", e.getMessage());
            assertEquals("def", e.getSuppressed()[0].getMessage());
        }
    }

    @Test
    void testCloseAutoCloseableArray() throws IOException {
        {
            var count = new AtomicInteger(0);
            IceaxeIoUtil.close(() -> count.addAndGet(1));
            assertEquals(1, count.get());
        }
        {
            var count = new AtomicInteger(0);
            IceaxeIoUtil.close(() -> count.addAndGet(1), () -> count.addAndGet(2));
            assertEquals(3, count.get());
        }
        {
            var count = new AtomicInteger(0);
            IceaxeIoUtil.close(() -> count.addAndGet(1), null, () -> count.addAndGet(2));
            assertEquals(3, count.get());
        }
    }

    @Test
    void testCloseAutoCloseableArrayEx() {
        {
            var count = new AtomicInteger(0);
            var e = assertThrows(IOException.class, () -> IceaxeIoUtil.close(() -> {
                count.addAndGet(1);
                throw new IOException("abc");
            }));
            assertEquals(1, count.get());

            assertEquals("abc", e.getMessage());
        }

        {
            var count = new AtomicInteger(0);
            var e = assertThrows(IOException.class, () -> IceaxeIoUtil.close(() -> {
                count.addAndGet(1);
                throw new IOException("abc");
            }, () -> {
                count.addAndGet(2);
            }));
            assertEquals(3, count.get());

            assertEquals("abc", e.getMessage());
        }
        {
            var count = new AtomicInteger(0);
            var e = assertThrows(IOException.class, () -> IceaxeIoUtil.close(() -> {
                count.addAndGet(1);
            }, () -> {
                count.addAndGet(2);
                throw new IOException("abc");
            }));
            assertEquals(3, count.get());

            assertEquals("abc", e.getMessage());
        }
        {
            var count = new AtomicInteger(0);
            var e = assertThrows(IOException.class, () -> IceaxeIoUtil.close(() -> {
                count.addAndGet(1);
                throw new IOException("abc");
            }, () -> {
                count.addAndGet(2);
                throw new IOException("def");
            }));
            assertEquals(3, count.get());

            assertEquals("abc", e.getMessage());
            assertEquals("def", e.getSuppressed()[0].getMessage());
        }
    }

    private static void assertLowTimeout(long expectedTime, TimeUnit expectedUmnit, Timeout actual) {
        var clazz = actual.getClass();
        try {
            var timeField = clazz.getDeclaredField("timeout");
            timeField.setAccessible(true);
            long actualTime = timeField.getLong(actual);
            assertEquals(expectedTime, actualTime);
            var unitField = clazz.getDeclaredField("unit");
            unitField.setAccessible(true);
            TimeUnit actualUnit = (TimeUnit) unitField.get(actual);
            assertEquals(expectedUmnit, actualUnit);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
