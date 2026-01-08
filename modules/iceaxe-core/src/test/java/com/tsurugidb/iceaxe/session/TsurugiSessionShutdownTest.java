/*
 * Copyright 2023-2026 Project Tsurugi.
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
package com.tsurugidb.iceaxe.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeTimeoutIOException;
import com.tsurugidb.iceaxe.test.low.TestFutureResponse;
import com.tsurugidb.iceaxe.test.low.TestLowSession;
import com.tsurugidb.iceaxe.util.IceaxeTimeoutCloseable;
import com.tsurugidb.iceaxe.util.TgTimeValue;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.ShutdownType;
import com.tsurugidb.tsubakuro.util.FutureResponse;

class TsurugiSessionShutdownTest {

    @FunctionalInterface
    private interface ShutdownCaller {
        void shutdown(TsurugiSession session, TgSessionShutdownType shutdownType) throws IOException, InterruptedException;
    }

    @Test
    void shudown_nothing_long() throws Exception {
        testShutdownNothing((session, type) -> session.shutdown(type, 1, TimeUnit.SECONDS));
    }

    @Test
    void shudown_nothing_TimeValue() throws Exception {
        testShutdownNothing((session, type) -> session.shutdown(type, TgTimeValue.of(1, TimeUnit.SECONDS)));
    }

    private void testShutdownNothing(ShutdownCaller caller) throws Exception {
        var sessionOption = TgSessionOption.of();
        try (var session = new TsurugiSession(null, sessionOption) {
            @Override
            protected FutureResponse<Void> shutdownLow(Session lowSession0, ShutdownType lowShutdownType) throws IOException {
                fail("called get()");
                return null;
            }
        }) {
            var child = new IceaxeTimeoutCloseable() {

                boolean closed = false;

                @Override
                public void close(long timeoutNanos) throws Exception {
                    this.closed = true;
                }
            };
            session.addChild(child);

            caller.shutdown(session, TgSessionShutdownType.NOTHING);

            assertTrue(child.closed);
        }
    }

    @Test
    void shudown_timeout_long() throws Exception {
        testShutdownTimeout((session, type) -> session.shutdown(type, 1, TimeUnit.SECONDS));
    }

    @Test
    void shudown_timeout_TimeValue() throws Exception {
        testShutdownTimeout((session, type) -> session.shutdown(type, TgTimeValue.of(1, TimeUnit.SECONDS)));
    }

    private void testShutdownTimeout(ShutdownCaller caller) throws Exception {
        var shutdownFuture = new TestFutureResponse<Void>();
        shutdownFuture.setExpectedTimeout(1, TimeUnit.SECONDS);
        shutdownFuture.setThrowTimeout(true);

        var sessionFuture = new TestFutureResponse<Session>() {
            @Override
            protected Session getInternal() {
                return new TestLowSession() {
                    @Override
                    public FutureResponse<Void> shutdown(ShutdownType type) throws IOException {
                        assertEquals(ShutdownType.GRACEFUL, type);
                        return shutdownFuture;
                    }
                };
            }
        };

        var sessionOption = TgSessionOption.of();
        sessionOption.setCloseShutdownType(TgSessionShutdownType.NOTHING);
        try (var session = new TsurugiSession(sessionFuture, sessionOption)) {
            var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> {
                caller.shutdown(session, TgSessionShutdownType.GRACEFUL);
            });
            assertEquals(IceaxeErrorCode.SESSION_SHUTDOWN_TIMEOUT, e.getDiagnosticCode());
        }

        assertTrue(shutdownFuture.isClosed());
    }

    @Test
    void shudown_closeTimeout_long() throws Exception {
        testShutdownCloseTimeout((session, type) -> session.shutdown(type, 1, TimeUnit.SECONDS));
    }

    @Test
    void shudown_closeTimeout_TimeValue() throws Exception {
        testShutdownCloseTimeout((session, type) -> session.shutdown(type, TgTimeValue.of(1, TimeUnit.SECONDS)));
    }

    private void testShutdownCloseTimeout(ShutdownCaller caller) throws Exception {
        var shutdownFuture = new TestFutureResponse<Void>();
        shutdownFuture.setExpectedCloseTimeout(1, TimeUnit.SECONDS);
        shutdownFuture.setThrowCloseTimeout(true);

        var sessionFuture = new TestFutureResponse<Session>() {
            @Override
            protected Session getInternal() {
                return new TestLowSession() {
                    @Override
                    public FutureResponse<Void> shutdown(ShutdownType type) throws IOException {
                        assertEquals(ShutdownType.GRACEFUL, type);
                        return shutdownFuture;
                    }
                };
            }
        };

        var sessionOption = TgSessionOption.of();
        sessionOption.setCloseShutdownType(TgSessionShutdownType.NOTHING);
        try (var session = new TsurugiSession(sessionFuture, sessionOption)) {
            var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> {
                caller.shutdown(session, TgSessionShutdownType.GRACEFUL);
            });
            assertEquals(IceaxeErrorCode.SESSION_SHUTDOWN_CLOSE_TIMEOUT, e.getDiagnosticCode());
        }

        assertTrue(shutdownFuture.isClosed());
    }
}
