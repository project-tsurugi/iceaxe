package com.tsurugidb.iceaxe.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.Timeout;

class IceaxeTimeoutTest {

    static class IceaxeServerResourceTestMock implements ServerResource {

        @Override
        public void setCloseTimeout(Timeout timeout) {
            try {
                timeout.waitFor(new IceaxeFutureResponseTestMock<>() {
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
            fail();
        }

        @Override
        public void close() throws ServerException, IOException, InterruptedException {
            fail();
        }
    }

    @Test
    void test() {
        var info = TgSessionInfo.of().timeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var target = new IceaxeTimeout(info, TgTimeoutKey.SESSION_CONNECT);

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
    void testToString() {
        var info = TgSessionInfo.of().timeout(TgTimeoutKey.SESSION_CONNECT, 123, TimeUnit.MILLISECONDS);
        var target = new IceaxeTimeout(info, TgTimeoutKey.SESSION_CONNECT);
        assertEquals("IceaxeTimeout{key=SESSION_CONNECT, value=null}", target.toString());

        target.get();
        assertEquals("IceaxeTimeout{key=SESSION_CONNECT, value=123milliseconds}", target.toString());

        target.set(456, TimeUnit.MICROSECONDS);
        assertEquals("IceaxeTimeout{key=SESSION_CONNECT, value=456microseconds}", target.toString());
    }
}
