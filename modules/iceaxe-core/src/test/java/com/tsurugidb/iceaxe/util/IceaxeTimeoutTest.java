package com.tsurugidb.iceaxe.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.test.low.TestFutureResponse;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.Timeout;

class IceaxeTimeoutTest {

    static class IceaxeServerResourceTestMock implements ServerResource {

        @Override
        public void setCloseTimeout(Timeout timeout) {
            try {
                timeout.waitFor(new TestFutureResponse<>() {
                    @Override
                    public Void get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                        assertCloseTimeout(timeout, unit);
                        return null;
                    }
                });
            } catch (IOException | ServerException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        protected void assertCloseTimeout(long timeout, TimeUnit unit) {
            fail("do override");
        }

        @Override
        public void close() throws ServerException, IOException, InterruptedException {
            fail("do override");
        }
    }

    @Test
    void test() {
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var target = new IceaxeTimeout(sessionOption, TgTimeoutKey.SESSION_CONNECT);

        // get 1回目
        target.apply(new IceaxeServerResourceTestMock() {
            @Override
            protected void assertCloseTimeout(long timeout, TimeUnit unit) {
                assertEquals(123L, timeout);
                assertEquals(TimeUnit.MILLISECONDS, unit);
            }
        });

        // get 2回目
        target.apply(new IceaxeServerResourceTestMock() {
            @Override
            protected void assertCloseTimeout(long timeout, TimeUnit unit) {
                assertEquals(123L, timeout);
                assertEquals(TimeUnit.MILLISECONDS, unit);
            }
        });

        target.set(456, TimeUnit.MICROSECONDS);
        target.apply(new IceaxeServerResourceTestMock() {
            @Override
            protected void assertCloseTimeout(long timeout, TimeUnit unit) {
                assertEquals(456L, timeout);
                assertEquals(TimeUnit.MICROSECONDS, unit);
            }
        });
    }

    @Test
    void getNanos() {
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var target = new IceaxeTimeout(sessionOption, TgTimeoutKey.SESSION_CONNECT);

        assertEquals(TimeUnit.MILLISECONDS.toNanos(123), target.getNanos());
    }

    @Test
    void constructLongUnit() {
        var target = new IceaxeTimeout(123, TimeUnit.MILLISECONDS);

        assertEquals(123L, target.get().value());
        assertEquals(TimeUnit.MILLISECONDS, target.get().unit());
    }

    @Test
    void testToString() {
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var target = new IceaxeTimeout(sessionOption, TgTimeoutKey.SESSION_CONNECT);
        assertEquals("IceaxeTimeout{key=SESSION_CONNECT, value=null}", target.toString());

        target.get();
        assertEquals("IceaxeTimeout{key=SESSION_CONNECT, value=123milliseconds}", target.toString());

        target.set(456, TimeUnit.MICROSECONDS);
        assertEquals("IceaxeTimeout{key=SESSION_CONNECT, value=456microseconds}", target.toString());
    }
}
