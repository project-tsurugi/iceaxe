package com.tsurugidb.iceaxe.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.Timeout;
import com.tsurugidb.iceaxe.exception.IceaxeServerExceptionTestMock;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

class IceaxeIoUtilTest {

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
    void testGetFromFutureEx() {
        var future = new IceaxeFutureResponseTestMock<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new IceaxeServerExceptionTestMock("abc", "def");
            }
        };
        var info = TgSessionInfo.of();
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrows(IOException.class, () -> IceaxeIoUtil.getFromFuture(future, timeout));
        assertEquals("def", actual.getMessage());
        assertEquals("abc", actual.getCause().getMessage());
    }

    @Test
    void testGetFromFutureIOEx() {
        var future = new IceaxeFutureResponseTestMock<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new InterruptedException("abc");
            }
        };
        var info = TgSessionInfo.of();
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.SESSION_CONNECT);
        var actual = assertThrows(IOException.class, () -> IceaxeIoUtil.getFromFuture(future, timeout));
        assertEquals("abc", actual.getCause().getMessage());
    }

    @Test
    void testGetFromFutureInTransaction() throws IOException, TsurugiTransactionException {
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
        var actual = IceaxeIoUtil.getFromFutureInTransaction(future, timeout);
        assertEquals("abc", actual);
    }

    @Test
    void testGetFromFutureInTransactionEx() {
        var future = new IceaxeFutureResponseTestMock<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new IceaxeServerExceptionTestMock("abc");
            }
        };
        var info = TgSessionInfo.of();
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.TRANSACTION_COMMIT);
        var actual = assertThrows(TsurugiTransactionException.class, () -> IceaxeIoUtil.getFromFutureInTransaction(future, timeout));
        assertEquals("abc", actual.getCause().getMessage());
    }

    @Test
    void testGetFromFutureInTransactionIOEx() {
        var future = new IceaxeFutureResponseTestMock<String>() {
            @Override
            public String get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                throw new TimeoutException("abc");
            }
        };
        var info = TgSessionInfo.of();
        var timeout = new IceaxeTimeout(info, TgTimeoutKey.TRANSACTION_COMMIT);
        var actual = assertThrows(IOException.class, () -> IceaxeIoUtil.getFromFutureInTransaction(future, timeout));
        assertEquals("abc", actual.getCause().getMessage());
    }

    @Test
    void testCheckAndCloseFutureInTransaction() throws IOException, TsurugiTransactionException {
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
        IceaxeIoUtil.checkAndCloseFutureInTransaction(future, checkTimeout, closeTimeout);
    }

    @Test
    void testCheckAndCloseFutureInTransactionEx() {
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
            var actual = assertThrows(TsurugiTransactionException.class, () -> IceaxeIoUtil.checkAndCloseFutureInTransaction(future, checkTimeout, closeTimeout));
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
            var actual = assertThrows(TsurugiTransactionException.class, () -> IceaxeIoUtil.checkAndCloseFutureInTransaction(future, checkTimeout, closeTimeout));
            assertEquals("abc", actual.getCause().getMessage());
        }
    }

    @Test
    void testCheckAndCloseFutureInTransactionIOEx() {
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
            var actual = assertThrows(IOException.class, () -> IceaxeIoUtil.checkAndCloseFutureInTransaction(future, checkTimeout, closeTimeout));
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
            var actual = assertThrows(IOException.class, () -> IceaxeIoUtil.checkAndCloseFutureInTransaction(future, checkTimeout, closeTimeout));
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
            closeableSet.add(() -> {
                count.addAndGet(1);
                throw new IOException("def");
            });
            var e = assertThrows(IOException.class, () -> IceaxeIoUtil.close(closeableSet, () -> {
                count.addAndGet(1);
            }));
            assertEquals(3, count.get());

            var message = e.getMessage();
            var message0 = e.getSuppressed()[0].getMessage();
            switch (message) {
            case "abc":
                assertEquals("def", message0);
                break;
            case "def":
                assertEquals("abc", message0);
                break;
            default:
                fail(message);
                break;
            }
        }
        {
            var count = new AtomicInteger(0);
            var closeableSet = new IceaxeCloseableSet();
            closeableSet.add(() -> {
                count.addAndGet(1);
                throw new Exception("abc");
            });
            var e = assertThrows(IOException.class, () -> IceaxeIoUtil.close(closeableSet, () -> {
                count.addAndGet(1);
            }));
            assertEquals(2, count.get());

            assertEquals("abc", e.getCause().getMessage());
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
        {
            var count = new AtomicInteger(0);
            var closeableSet = new IceaxeCloseableSet();
            closeableSet.add(() -> {
                count.addAndGet(1);
                throw new IOException("def");
            });
            closeableSet.add(() -> {
                count.addAndGet(1);
                throw new IOException("ghi");
            });
            var e = assertThrows(IOException.class, () -> IceaxeIoUtil.close(closeableSet, () -> {
                count.addAndGet(1);
                throw new IOException("abc");
            }));
            assertEquals(3, count.get());

            assertEquals("abc", e.getMessage());
            var message0 = e.getSuppressed()[0].getMessage();
            var message1 = e.getSuppressed()[1].getMessage();
            switch (message0) {
            case "def":
                assertEquals("ghi", message1);
                break;
            case "ghi":
                assertEquals("def", message1);
                break;
            default:
                fail(message0);
                break;
            }
        }
        {
            var count = new AtomicInteger(0);
            var closeableSet = new IceaxeCloseableSet();
            closeableSet.add(() -> {
                count.addAndGet(1);
                throw new Exception("def");
            });
            closeableSet.add(() -> {
                count.addAndGet(1);
                throw new Exception("ghi");
            });
            var e = assertThrows(IOException.class, () -> IceaxeIoUtil.close(closeableSet, () -> {
                count.addAndGet(1);
                throw new IOException("abc");
            }));
            assertEquals(3, count.get());

            assertEquals("abc", e.getMessage());
            var message0 = e.getSuppressed()[0].getMessage();
            var message1 = e.getSuppressed()[1].getMessage();
            switch (message0) {
            case "def":
                assertEquals("ghi", message1);
                break;
            case "ghi":
                assertEquals("def", message1);
                break;
            default:
                fail(message0);
                break;
            }
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

        {
            var count = new AtomicInteger(0);
            var e = assertThrows(IOException.class, () -> IceaxeIoUtil.close(() -> {
                count.addAndGet(1);
                throw new Exception("abc");
            }, () -> {
                count.addAndGet(2);
                throw new Exception("def");
            }));
            assertEquals(3, count.get());

            assertEquals("abc", e.getCause().getMessage());
            assertEquals("def", e.getSuppressed()[0].getMessage());
        }
    }

    @Test
    void testCloseInTransaction() throws IOException, TsurugiTransactionException {
        {
            var count = new AtomicInteger(0);
            IceaxeIoUtil.closeInTransaction(() -> count.addAndGet(1));
            assertEquals(1, count.get());
        }
        {
            var count = new AtomicInteger(0);
            IceaxeIoUtil.closeInTransaction(() -> count.addAndGet(1), () -> count.addAndGet(2));
            assertEquals(3, count.get());
        }
        {
            var count = new AtomicInteger(0);
            IceaxeIoUtil.closeInTransaction(() -> count.addAndGet(1), null, () -> count.addAndGet(2));
            assertEquals(3, count.get());
        }
    }

    @Test
    void testCloseInTransactionEx() {
        {
            var count = new AtomicInteger(0);
            var e = assertThrows(TsurugiTransactionException.class, () -> IceaxeIoUtil.closeInTransaction(() -> {
                count.addAndGet(1);
                throw new IceaxeServerExceptionTestMock("abc");
            }));
            assertEquals(1, count.get());

            assertEquals("abc", e.getCause().getMessage());
        }

        {
            var count = new AtomicInteger(0);
            var e = assertThrows(TsurugiTransactionException.class, () -> IceaxeIoUtil.closeInTransaction(() -> {
                count.addAndGet(1);
                throw new IceaxeServerExceptionTestMock("abc");
            }, () -> {
                count.addAndGet(2);
            }));
            assertEquals(3, count.get());

            assertEquals("abc", e.getCause().getMessage());
        }
        {
            var count = new AtomicInteger(0);
            var e = assertThrows(TsurugiTransactionException.class, () -> IceaxeIoUtil.closeInTransaction(() -> {
                count.addAndGet(1);
            }, () -> {
                count.addAndGet(2);
                throw new IceaxeServerExceptionTestMock("abc");
            }));
            assertEquals(3, count.get());

            assertEquals("abc", e.getCause().getMessage());
        }
        {
            var count = new AtomicInteger(0);
            var e = assertThrows(TsurugiTransactionException.class, () -> IceaxeIoUtil.closeInTransaction(() -> {
                count.addAndGet(1);
                throw new IceaxeServerExceptionTestMock("abc");
            }, () -> {
                count.addAndGet(2);
                throw new IceaxeServerExceptionTestMock("def");
            }, () -> {
                count.addAndGet(4);
                throw new IceaxeServerExceptionTestMock("ghi");
            }));
            assertEquals(7, count.get());

            assertEquals("abc", e.getCause().getMessage());
            var ex0 = e.getSuppressed()[0];
            assertInstanceOf(ServerException.class, ex0);
            assertEquals("def", ex0.getMessage());
            var ex1 = e.getSuppressed()[1];
            assertInstanceOf(ServerException.class, ex1);
            assertEquals("ghi", ex1.getMessage());
        }
        {
            var count = new AtomicInteger(0);
            var e = assertThrows(TsurugiTransactionException.class, () -> IceaxeIoUtil.closeInTransaction(() -> {
                count.addAndGet(1);
                throw new IceaxeServerExceptionTestMock("abc");
            }, () -> {
                count.addAndGet(2);
                throw new IOException("def");
            }));
            assertEquals(3, count.get());

            assertEquals("abc", e.getCause().getMessage());
            var ex0 = e.getSuppressed()[0];
            assertInstanceOf(IOException.class, ex0);
            assertEquals("def", ex0.getMessage());
        }
        {
            var count = new AtomicInteger(0);
            var e = assertThrows(TsurugiTransactionException.class, () -> IceaxeIoUtil.closeInTransaction(() -> {
                count.addAndGet(1);
                throw new IOException("abc");
            }, () -> {
                count.addAndGet(2);
                throw new IceaxeServerExceptionTestMock("def");
            }, () -> {
                count.addAndGet(4);
                throw new IOException("ghi");
            }));
            assertEquals(7, count.get());

            assertEquals("def", e.getCause().getMessage());
            var ex0 = e.getSuppressed()[0];
            assertInstanceOf(IOException.class, ex0);
            assertEquals("abc", ex0.getMessage());
            var ex1 = e.getSuppressed()[1];
            assertInstanceOf(IOException.class, ex1);
            assertEquals("ghi", ex1.getMessage());
        }
    }

    @Test
    void testCloseInTransactionIOEx() {
        {
            var count = new AtomicInteger(0);
            var e = assertThrows(IOException.class, () -> IceaxeIoUtil.closeInTransaction(() -> {
                count.addAndGet(1);
                throw new IOException("abc");
            }));
            assertEquals(1, count.get());

            assertEquals("abc", e.getMessage());
        }

        {
            var count = new AtomicInteger(0);
            var e = assertThrows(IOException.class, () -> IceaxeIoUtil.closeInTransaction(() -> {
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
            var e = assertThrows(IOException.class, () -> IceaxeIoUtil.closeInTransaction(() -> {
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
            var e = assertThrows(IOException.class, () -> IceaxeIoUtil.closeInTransaction(() -> {
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
        {
            var count = new AtomicInteger(0);
            var e = assertThrows(IOException.class, () -> IceaxeIoUtil.closeInTransaction(() -> {
                count.addAndGet(1);
                throw new Exception("abc");
            }, () -> {
                count.addAndGet(2);
                throw new Exception("def");
            }));
            assertEquals(3, count.get());

            assertEquals("abc", e.getCause().getMessage());
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
