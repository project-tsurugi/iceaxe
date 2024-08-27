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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.exception.IceaxeServerExceptionTestMock;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.tsubakuro.exception.ServerException;

class IceaxeCloseableSetTest {

    @Test
    void test0() {
        var target = new IceaxeCloseableSet();
        List<Throwable> result = target.close(0);

        assertEquals(0, result.size());
    }

    @Test
    void test1() {
        var target = new IceaxeCloseableSet();
        var count = new AtomicInteger(0);

        var closeable1 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) throws IOException {
                count.addAndGet(1);
                target.remove(this);
            }
        };
        target.add(closeable1);

        List<Throwable> result = target.close(0);
        assertEquals(1, count.get());

        assertEquals(0, result.size());
    }

    @Test
    void test1Ex() {
        var target = new IceaxeCloseableSet();
        var count = new AtomicInteger(0);

        var closeable1 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) throws IOException {
                count.addAndGet(1);
                throw new IOException("abc");
            }
        };
        target.add(closeable1);

        List<Throwable> result = target.close(0);
        assertEquals(1, count.get());

        assertEquals(1, result.size());
        var e = (IOException) result.get(0);
        assertEquals("abc", e.getMessage());
    }

    @Test
    void test2() {
        var target = new IceaxeCloseableSet();
        var count = new AtomicInteger(0);

        var closeable1 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) throws IOException {
                count.addAndGet(1);
                target.remove(this);
            }
        };
        target.add(closeable1);
        var closeable2 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) throws IOException {
                count.addAndGet(1);
                target.remove(this);
            }
        };
        target.add(closeable2);

        List<Throwable> result = target.close(0);
        assertEquals(2, count.get());

        assertEquals(0, result.size());
    }

    @Test
    void test2Ex1() {
        var target = new IceaxeCloseableSet();
        var count = new AtomicInteger(0);

        var closeable1 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) throws IOException {
                count.addAndGet(1);
                throw new IOException("abc");
            }
        };
        target.add(closeable1);
        var closeable2 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) throws IOException {
                count.addAndGet(1);
                target.remove(this);
            }
        };
        target.add(closeable2);

        List<Throwable> result = target.close(0);
        assertEquals(2, count.get());

        assertEquals(1, result.size());
        var e = (IOException) result.get(0);
        assertEquals("abc", e.getMessage());
    }

    @Test
    void test2Ex2() {
        var target = new IceaxeCloseableSet();
        var count = new AtomicInteger(0);

        var closeable1 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) throws IOException {
                count.addAndGet(1);
                target.remove(this);
            }
        };
        target.add(closeable1);
        var closeable2 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) throws IOException {
                count.addAndGet(1);
                throw new IOException("abc");
            }
        };
        target.add(closeable2);

        List<Throwable> result = target.close(0);
        assertEquals(2, count.get());

        assertEquals(1, result.size());
        var e = (IOException) result.get(0);
        assertEquals("abc", e.getMessage());
    }

    @Test
    void test2Ex3() {
        var target = new IceaxeCloseableSet();
        var count = new AtomicInteger(0);

        var closeable1 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) throws IOException {
                count.addAndGet(1);
                throw new IOException("abc");
            }
        };
        target.add(closeable1);
        var closeable2 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) throws IOException {
                count.addAndGet(1);
                throw new IOException("def");
            }
        };
        target.add(closeable2);

        List<Throwable> result = target.close(0);
        assertEquals(2, count.get());

        assertEquals(2, result.size());
        var e0 = (IOException) result.get(0);
        var e1 = (IOException) result.get(1);
        assertEquals("abc", e0.getMessage());
        assertEquals("def", e1.getMessage());
    }

    private static final IceaxeErrorCode CLOSE_IN_TX_ERROR = IceaxeErrorCode.TX_COMMIT_CHILD_CLOSE_ERROR;

    @Test
    void testCloseInTransaction0() throws IOException, InterruptedException, TsurugiTransactionException {
        var target = new IceaxeCloseableSet();
        assertEquals(0, target.size());
        target.closeInTransaction(0, CLOSE_IN_TX_ERROR);
        assertEquals(0, target.size());

        var count = new AtomicInteger(0);
        var closeable1 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) throws IOException {
                count.addAndGet(1);
                target.remove(this);
            }
        };
        target.add(closeable1);
        assertEquals(1, target.size());

        var closeable2 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) throws IOException {
                count.addAndGet(1);
                target.remove(this);
            }
        };
        target.add(closeable2);
        assertEquals(2, target.size());

        var closeable3 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) throws IOException {
                count.addAndGet(1);
                target.remove(this);
            }
        };
        target.add(closeable3);
        assertEquals(3, target.size());

        closeable2.close(0);
        assertEquals(1, count.get());
        assertEquals(2, target.size());

        closeable1.close(0);
        assertEquals(2, count.get());
        assertEquals(1, target.size());

        closeable3.close(0);
        assertEquals(3, count.get());
        assertEquals(0, target.size());

        target.closeInTransaction(0, CLOSE_IN_TX_ERROR);
        assertEquals(3, count.get());
        assertEquals(0, target.size());
    }

    @Test
    void testCloseInTransaction() throws IOException, InterruptedException, TsurugiTransactionException {
        var target = new IceaxeCloseableSet();
        assertEquals(0, target.size());
        target.closeInTransaction(0, CLOSE_IN_TX_ERROR);
        assertEquals(0, target.size());

        var count = new AtomicInteger(0);
        var closeable1 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) throws IOException {
                count.addAndGet(1);
                target.remove(this);
            }
        };
        target.add(closeable1);
        assertEquals(1, target.size());

        var closeable2 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) throws IOException {
                count.addAndGet(1);
                target.remove(this);
            }
        };
        target.add(closeable2);
        assertEquals(2, target.size());

        target.closeInTransaction(0, CLOSE_IN_TX_ERROR);
        assertEquals(2, count.get());
        assertEquals(0, target.size());
    }

    // 最初の例外がIOException
    @Test
    void testCloseInTransactionIOEx() {
        var target = new IceaxeCloseableSet();

        var closeable1 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) throws IOException {
                throw new IOException("abc");
            }
        };
        target.add(closeable1);
        assertEquals(1, target.size());

        var closeable2 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) {
                throw new RuntimeException("def");
            }
        };
        target.add(closeable2);
        assertEquals(2, target.size());

        var e = assertThrowsExactly(IOException.class, () -> {
            target.closeInTransaction(0, CLOSE_IN_TX_ERROR);
        });
        assertEquals("abc", e.getMessage());
        assertEquals(1, e.getSuppressed().length);
        var s0 = e.getSuppressed()[0];
        assertInstanceOf(RuntimeException.class, s0);
        assertEquals("def", s0.getMessage());
        assertEquals(0, target.size());
    }

    // 最初の例外がInterruptedException
    @Test
    void testCloseInTransactionInterruptedEx() {
        var target = new IceaxeCloseableSet();

        var closeable1 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) throws InterruptedException {
                throw new InterruptedException("abc");
            }
        };
        target.add(closeable1);
        assertEquals(1, target.size());

        var closeable2 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) {
                throw new RuntimeException("def");
            }
        };
        target.add(closeable2);
        assertEquals(2, target.size());

        var e = assertThrowsExactly(InterruptedException.class, () -> {
            target.closeInTransaction(0, CLOSE_IN_TX_ERROR);
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

        var closeable1 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) throws TsurugiTransactionException {
                var e = new IceaxeServerExceptionTestMock("abc", 123);
                throw new TsurugiTransactionException(e);
            }
        };
        target.add(closeable1);
        assertEquals(1, target.size());

        var closeable2 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) {
                var e = new IceaxeServerExceptionTestMock("def", 456);
                var t = new TsurugiTransactionException(e);
                throw new TsurugiTransactionRuntimeException(t);
            }
        };
        target.add(closeable2);
        assertEquals(2, target.size());

        var closeable3 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) throws Exception {
                throw new IceaxeServerExceptionTestMock("ghi", 789);
            }
        };
        target.add(closeable3);
        assertEquals(3, target.size());

        var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
            target.closeInTransaction(0, CLOSE_IN_TX_ERROR);
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
        var closeable1 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) {
                throw new TsurugiTransactionRuntimeException(t);
            }
        };
        target.add(closeable1);
        assertEquals(1, target.size());

        var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
            target.closeInTransaction(0, CLOSE_IN_TX_ERROR);
        });
        assertSame(t, e);
        assertEquals(0, target.size());
    }

    // 最初の例外がServerException
    @Test
    void testCloseInTransactionEx3() {
        var target = new IceaxeCloseableSet();

        var t = new IceaxeServerExceptionTestMock("abc", 123);
        var closeable1 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) throws ServerException {
                throw t;
            }
        };
        target.add(closeable1);
        assertEquals(1, target.size());

        var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
            target.closeInTransaction(0, CLOSE_IN_TX_ERROR);
        });
        assertSame(t, e.getCause());
        assertEquals(0, target.size());
    }

    // 最初の例外がRuntimeException
    @Test
    void testCloseInTransactionEx4() {
        var target = new IceaxeCloseableSet();

        var closeable1 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) {
                throw new RuntimeException("abc");
            }
        };
        target.add(closeable1);
        assertEquals(1, target.size());

        var e = assertThrowsExactly(RuntimeException.class, () -> {
            target.closeInTransaction(0, CLOSE_IN_TX_ERROR);
        });
        assertEquals("abc", e.getMessage());
        assertEquals(0, target.size());
    }

    // 最初の例外がその他のException
    @Test
    void testCloseInTransactionEx5() {
        var target = new IceaxeCloseableSet();

        var t = new Exception("abc");
        var closeable1 = new IceaxeTimeoutCloseable() {
            @Override
            public void close(long timeoutNanos) throws Exception {
                throw t;
            }
        };
        target.add(closeable1);
        assertEquals(1, target.size());

        var e = assertThrowsExactly(IceaxeIOException.class, () -> {
            target.closeInTransaction(0, CLOSE_IN_TX_ERROR);
        });
        assertEquals(CLOSE_IN_TX_ERROR, e.getDiagnosticCode());
        assertEquals(CLOSE_IN_TX_ERROR.getMessage() + ": abc", e.getMessage());
        assertSame(t, e.getCause());
        assertEquals(0, target.size());
    }
}
