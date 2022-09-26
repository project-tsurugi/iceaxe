package com.tsurugidb.iceaxe.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeServerExceptionTestMock;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.tsubakuro.exception.ServerException;

class IceaxeCloseableSetTest {

    @Test
    void test0() {
        var target = new IceaxeCloseableSet();
        List<Throwable> result = target.close();

        assertEquals(0, result.size());
    }

    @Test
    void test1() {
        var target = new IceaxeCloseableSet();
        var count = new AtomicInteger(0);

        var closeable1 = new Closeable() {
            @Override
            public void close() throws IOException {
                count.addAndGet(1);
                target.remove(this);
            }
        };
        target.add(closeable1);

        List<Throwable> result = target.close();
        assertEquals(1, count.get());

        assertEquals(0, result.size());
    }

    @Test
    void test1Ex() {
        var target = new IceaxeCloseableSet();
        var count = new AtomicInteger(0);

        var closeable1 = new Closeable() {
            @Override
            public void close() throws IOException {
                count.addAndGet(1);
                throw new IOException("abc");
            }
        };
        target.add(closeable1);

        List<Throwable> result = target.close();
        assertEquals(1, count.get());

        assertEquals(1, result.size());
        var e = (IOException) result.get(0);
        assertEquals("abc", e.getMessage());
    }

    @Test
    void test2() {
        var target = new IceaxeCloseableSet();
        var count = new AtomicInteger(0);

        var closeable1 = new Closeable() {
            @Override
            public void close() throws IOException {
                count.addAndGet(1);
                target.remove(this);
            }
        };
        target.add(closeable1);
        var closeable2 = new Closeable() {
            @Override
            public void close() throws IOException {
                count.addAndGet(1);
                target.remove(this);
            }
        };
        target.add(closeable2);

        List<Throwable> result = target.close();
        assertEquals(2, count.get());

        assertEquals(0, result.size());
    }

    @Test
    void test2Ex1() {
        var target = new IceaxeCloseableSet();
        var count = new AtomicInteger(0);

        var closeable1 = new Closeable() {
            @Override
            public void close() throws IOException {
                count.addAndGet(1);
                throw new IOException("abc");
            }
        };
        target.add(closeable1);
        var closeable2 = new Closeable() {
            @Override
            public void close() throws IOException {
                count.addAndGet(1);
                target.remove(this);
            }
        };
        target.add(closeable2);

        List<Throwable> result = target.close();
        assertEquals(2, count.get());

        assertEquals(1, result.size());
        var e = (IOException) result.get(0);
        assertEquals("abc", e.getMessage());
    }

    @Test
    void test2Ex2() {
        var target = new IceaxeCloseableSet();
        var count = new AtomicInteger(0);

        var closeable1 = new Closeable() {
            @Override
            public void close() throws IOException {
                count.addAndGet(1);
                target.remove(this);
            }
        };
        target.add(closeable1);
        var closeable2 = new Closeable() {
            @Override
            public void close() throws IOException {
                count.addAndGet(1);
                throw new IOException("abc");
            }
        };
        target.add(closeable2);

        List<Throwable> result = target.close();
        assertEquals(2, count.get());

        assertEquals(1, result.size());
        var e = (IOException) result.get(0);
        assertEquals("abc", e.getMessage());
    }

    @Test
    void test2Ex3() {
        var target = new IceaxeCloseableSet();
        var count = new AtomicInteger(0);

        var closeable1 = new Closeable() {
            @Override
            public void close() throws IOException {
                count.addAndGet(1);
                throw new IOException("abc");
            }
        };
        target.add(closeable1);
        var closeable2 = new Closeable() {
            @Override
            public void close() throws IOException {
                count.addAndGet(1);
                throw new IOException("def");
            }
        };
        target.add(closeable2);

        List<Throwable> result = target.close();
        assertEquals(2, count.get());

        assertEquals(2, result.size());
        var e0 = (IOException) result.get(0);
        var e1 = (IOException) result.get(1);
        assertEquals("abc", e0.getMessage());
        assertEquals("def", e1.getMessage());
    }

    @Test
    void testCloseInTransaction0() throws IOException, TsurugiTransactionException {
        var target = new IceaxeCloseableSet();
        assertEquals(0, target.size());
        target.closeInTransaction();
        assertEquals(0, target.size());

        var count = new AtomicInteger(0);
        var closeable1 = new Closeable() {
            @Override
            public void close() throws IOException {
                count.addAndGet(1);
                target.remove(this);
            }
        };
        target.add(closeable1);
        assertEquals(1, target.size());

        var closeable2 = new Closeable() {
            @Override
            public void close() throws IOException {
                count.addAndGet(1);
                target.remove(this);
            }
        };
        target.add(closeable2);
        assertEquals(2, target.size());

        var closeable3 = new Closeable() {
            @Override
            public void close() throws IOException {
                count.addAndGet(1);
                target.remove(this);
            }
        };
        target.add(closeable3);
        assertEquals(3, target.size());

        closeable2.close();
        assertEquals(1, count.get());
        assertEquals(2, target.size());

        closeable1.close();
        assertEquals(2, count.get());
        assertEquals(1, target.size());

        closeable3.close();
        assertEquals(3, count.get());
        assertEquals(0, target.size());

        target.closeInTransaction();
        assertEquals(3, count.get());
        assertEquals(0, target.size());
    }

    @Test
    void testCloseInTransaction() throws IOException, TsurugiTransactionException {
        var target = new IceaxeCloseableSet();
        assertEquals(0, target.size());
        target.closeInTransaction();
        assertEquals(0, target.size());

        var count = new AtomicInteger(0);
        var closeable1 = new Closeable() {
            @Override
            public void close() throws IOException {
                count.addAndGet(1);
                target.remove(this);
            }
        };
        target.add(closeable1);
        assertEquals(1, target.size());

        var closeable2 = new Closeable() {
            @Override
            public void close() throws IOException {
                count.addAndGet(1);
                target.remove(this);
            }
        };
        target.add(closeable2);
        assertEquals(2, target.size());

        target.closeInTransaction();
        assertEquals(2, count.get());
        assertEquals(0, target.size());
    }

    // 最初の例外がIOException
    @Test
    void testCloseInTransactionIOEx() {
        var target = new IceaxeCloseableSet();

        var closeable1 = new Closeable() {
            @Override
            public void close() throws IOException {
                throw new IOException("abc");
            }
        };
        target.add(closeable1);
        assertEquals(1, target.size());

        var closeable2 = new Closeable() {
            @Override
            public void close() {
                throw new RuntimeException("def");
            }
        };
        target.add(closeable2);
        assertEquals(2, target.size());

        var e = assertThrows(IOException.class, () -> {
            target.closeInTransaction();
        });
        assertEquals("abc", e.getMessage());
        assertEquals(1, e.getSuppressed().length);
        var s0 = e.getSuppressed()[0];
        assertInstanceOf(RuntimeException.class, s0);
        assertEquals("def", s0.getMessage());
        assertEquals(0, target.size());
    }

    // 最初の例外がTsurugiTransactionException
    @Test
    void testCloseInTransactionEx1() {
        var target = new IceaxeCloseableSet();

        var closeable1 = new AutoCloseable() {
            @Override
            public void close() throws TsurugiTransactionException {
                var e = new IceaxeServerExceptionTestMock("abc", 123);
                throw new TsurugiTransactionException(e);
            }
        };
        target.add(closeable1);
        assertEquals(1, target.size());

        var closeable2 = new Closeable() {
            @Override
            public void close() {
                var e = new IceaxeServerExceptionTestMock("def", 456);
                var t = new TsurugiTransactionException(e);
                throw new TsurugiTransactionRuntimeException(t);
            }
        };
        target.add(closeable2);
        assertEquals(2, target.size());

        var closeable3 = new AutoCloseable() {
            @Override
            public void close() throws Exception {
                throw new IceaxeServerExceptionTestMock("ghi", 789);
            }
        };
        target.add(closeable3);
        assertEquals(3, target.size());

        var e = assertThrows(TsurugiTransactionException.class, () -> {
            target.closeInTransaction();
        });
        assertEquals("MOCK_123: abc", e.getMessage());
        assertEquals(2, e.getSuppressed().length);
        var s0 = e.getSuppressed()[0];
        assertInstanceOf(TsurugiTransactionRuntimeException.class, s0);
        assertEquals("MOCK_456: def", s0.getMessage());
        var s1 = e.getSuppressed()[1];
        assertInstanceOf(ServerException.class, s1);
        assertEquals("ghi", s1.getMessage());
        assertEquals(0, target.size());
    }

    // 最初の例外がTsurugiTransactionRuntimeException
    @Test
    void testCloseInTransactionEx2() {
        var target = new IceaxeCloseableSet();

        var t = new TsurugiTransactionException(new IceaxeServerExceptionTestMock("abc", 123));
        var closeable1 = new Closeable() {
            @Override
            public void close() {
                throw new TsurugiTransactionRuntimeException(t);
            }
        };
        target.add(closeable1);
        assertEquals(1, target.size());

        var e = assertThrows(TsurugiTransactionException.class, () -> {
            target.closeInTransaction();
        });
        assertSame(t, e);
        assertEquals(0, target.size());
    }

    // 最初の例外がServerException
    @Test
    void testCloseInTransactionEx3() {
        var target = new IceaxeCloseableSet();

        var t = new IceaxeServerExceptionTestMock("abc", 123);
        var closeable1 = new AutoCloseable() {
            @Override
            public void close() throws ServerException {
                throw t;
            }
        };
        target.add(closeable1);
        assertEquals(1, target.size());

        var e = assertThrows(TsurugiTransactionException.class, () -> {
            target.closeInTransaction();
        });
        assertSame(t, e.getCause());
        assertEquals(0, target.size());
    }

    // 最初の例外がRuntimeException
    @Test
    void testCloseInTransactionEx4() {
        var target = new IceaxeCloseableSet();

        var closeable1 = new Closeable() {
            @Override
            public void close() {
                throw new RuntimeException("abc");
            }
        };
        target.add(closeable1);
        assertEquals(1, target.size());

        var e = assertThrows(RuntimeException.class, () -> {
            target.closeInTransaction();
        });
        assertEquals("abc", e.getMessage());
        assertEquals(0, target.size());
    }

    // 最初の例外がその他のException
    @Test
    void testCloseInTransactionEx5() {
        var target = new IceaxeCloseableSet();

        var t = new Exception("abc");
        var closeable1 = new AutoCloseable() {
            @Override
            public void close() throws Exception {
                throw t;
            }
        };
        target.add(closeable1);
        assertEquals(1, target.size());

        var e = assertThrows(IOException.class, () -> {
            target.closeInTransaction();
        });
        assertEquals("abc", e.getMessage());
        assertSame(t, e.getCause());
        assertEquals(0, target.size());
    }
}
