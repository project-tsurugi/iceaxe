package com.tsurugidb.iceaxe.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeTimeoutIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.test.low.TestFutureResponse;
import com.tsurugidb.tsubakuro.common.Session;

class TsurugiSessionConnectTimeoutTest {

    @Test
    void connectTimeout_default() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);

        testConnectTimeout(sessionOption, null);
    }

    @Test
    void connectTimeout_specified() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.SESSION_CONNECT, 1, TimeUnit.SECONDS);

        testConnectTimeout(sessionOption, null);
    }

    @Test
    void connectTimeout_set() throws Exception {
        var sessionOption = TgSessionOption.of();

        testConnectTimeout(sessionOption, session -> session.setConnectTimeout(1, TimeUnit.SECONDS));
    }

    private void testConnectTimeout(TgSessionOption sessionOption, Consumer<TsurugiSession> modifier) throws Exception {
        var future = new TestFutureResponse<Session>();
        future.setExpectedTimeout(1, TimeUnit.SECONDS);
        future.setThrowTimeout(true);

        try (var session = new TsurugiSession(future, sessionOption)) {
            if (modifier != null) {
                modifier.accept(session);
            }

            var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> session.getLowSession());
            assertEquals(IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, e.getDiagnosticCode());
        }

        assertTrue(future.isClosed());
    }

    @Test
    void futureCloseTimeout_default() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);

        testFutureCloseTimeout(sessionOption, null);
    }

    @Test
    void futureCloseTimeout_specified() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.SESSION_CONNECT, 1, TimeUnit.SECONDS);

        testFutureCloseTimeout(sessionOption, null);
    }

    @Test
    void futureCloseTimeout_set() throws Exception {
        var sessionOption = TgSessionOption.of();

        testFutureCloseTimeout(sessionOption, session -> session.setConnectTimeout(1, TimeUnit.SECONDS));
    }

    private void testFutureCloseTimeout(TgSessionOption sessionOption, Consumer<TsurugiSession> modifier) throws Exception {
        var future = new TestFutureResponse<Session>();
        future.setExpectedCloseTimeout(1, TimeUnit.SECONDS);
        future.setThrowCloseTimeout(true);

        try (var session = new TsurugiSession(future, sessionOption)) {
            if (modifier != null) {
                modifier.accept(session);
            }

            var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> session.getLowSession());
            assertEquals(IceaxeErrorCode.SESSION_CLOSE_TIMEOUT, e.getDiagnosticCode());

            future.setExpectedCloseTimeout(sessionOption.getTimeout(TgTimeoutKey.DEFAULT));
            future.setThrowCloseTimeout(false);
        }

        assertTrue(future.isClosed());
    }
}
