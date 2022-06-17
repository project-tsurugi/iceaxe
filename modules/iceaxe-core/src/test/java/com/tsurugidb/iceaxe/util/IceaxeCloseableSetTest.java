package com.tsurugidb.iceaxe.util;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

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
        if ("abc".equals(e0.getMessage())) {
            assertEquals("def", e1.getMessage());
        } else if ("def".equals(e0.getMessage())) {
            assertEquals("abc", e1.getMessage());
        } else {
            fail(e0.getMessage());
        }
    }
}
