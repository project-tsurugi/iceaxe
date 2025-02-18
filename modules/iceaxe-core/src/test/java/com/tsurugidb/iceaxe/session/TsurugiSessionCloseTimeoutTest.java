/*
 * Copyright 2023-2025 Project Tsurugi.
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

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeTimeoutIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.test.low.TestFutureResponse;
import com.tsurugidb.iceaxe.test.low.TestLowSession;
import com.tsurugidb.iceaxe.test.low.TestSqlClient;
import com.tsurugidb.iceaxe.util.IceaxeTimeoutCloseable;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.sql.SqlClient;

class TsurugiSessionCloseTimeoutTest {

    @Test
    void futureCloseTimeout_default() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);

        testFutureCloseTimeout(sessionOption, null);
    }

    @Test
    void futureCloseTimeout_specified() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.SESSION_CLOSE, 1, TimeUnit.SECONDS);

        testFutureCloseTimeout(sessionOption, null);
    }

    @Test
    void futureCloseTimeout_set() throws Exception {
        var sessionOption = TgSessionOption.of();

        testFutureCloseTimeout(sessionOption, session -> session.setCloseTimeout(1, TimeUnit.SECONDS));
    }

    private void testFutureCloseTimeout(TgSessionOption sessionOption, Consumer<TsurugiSession> modifier) throws Exception {
        var future = new TestFutureResponse<Session>();
        future.setExpectedCloseTimeout(1, TimeUnit.SECONDS);
        future.setThrowCloseTimeout(true);

        var session = new TsurugiSession(future, sessionOption);
        if (modifier != null) {
            modifier.accept(session);
        }

        var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> session.close());
        assertEquals(IceaxeErrorCode.SESSION_CLOSE_TIMEOUT, e.getDiagnosticCode());

        assertTrue(future.isClosed());
    }

    @Test
    void lowCloseTimeout_default() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);

        testLowCloseTimeout(sessionOption, null);
    }

    @Test
    void lowCloseTimeout_specified() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.SESSION_CLOSE, 1, TimeUnit.SECONDS);

        testLowCloseTimeout(sessionOption, null);
    }

    @Test
    void lowCloseTimeout_set() throws Exception {
        var sessionOption = TgSessionOption.of();

        testLowCloseTimeout(sessionOption, session -> session.setCloseTimeout(1, TimeUnit.SECONDS));
    }

    private void testLowCloseTimeout(TgSessionOption sessionOption, Consumer<TsurugiSession> modifier) throws Exception {
        var future = new TestFutureResponse<Session>() {
            @Override
            protected Session getInternal() {
                return new TestLowSession();
            }
        };

        var session = new TsurugiSession(future, sessionOption) {
            @Override
            protected SqlClient newSqlClient(Session lowSession) {
                return new TestSqlClient(lowSession);
            }
        };
        if (modifier != null) {
            modifier.accept(session);
        }

        var sqlClient = (TestSqlClient) session.getLowSqlClient();
        var lowSession = (TestLowSession) sqlClient.getSession();
        lowSession.setExpectedCloseTimeout(1, TimeUnit.SECONDS);
        lowSession.setThrowCloseTimeout(true);

        var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> session.close());
        assertEquals(IceaxeErrorCode.SESSION_CLOSE_TIMEOUT, e.getDiagnosticCode());

        assertTrue(future.isClosed());
        assertTrue(sqlClient.isClosed());
        assertTrue(lowSession.isClosed());
    }

    @Test
    void closeTimeoutWithChild_default() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);

        testCloseTimeoutWithChild(sessionOption, null);
    }

    @Test
    void closeTimeoutWithChild_specified() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 10, TimeUnit.SECONDS);
        sessionOption.setTimeout(TgTimeoutKey.SESSION_CLOSE, 1, TimeUnit.SECONDS);

        testCloseTimeoutWithChild(sessionOption, null);
    }

    @Test
    void closeTimeoutWithChild_set() throws Exception {
        var sessionOption = TgSessionOption.of();

        testCloseTimeoutWithChild(sessionOption, session -> session.setCloseTimeout(1, TimeUnit.SECONDS));
    }

    private void testCloseTimeoutWithChild(TgSessionOption sessionOption, Consumer<TsurugiSession> modifier) throws Exception {
        long childSleep = TimeUnit.MILLISECONDS.toNanos(50);
        var child = new IceaxeTimeoutCloseable() {

            boolean closed = false;

            @Override
            public void close(long timeoutNanos) throws Exception {
                this.closed = true;
                TimeUnit.NANOSECONDS.sleep(childSleep * 2);
            }
        };

        var future = new TestFutureResponse<Session>() {
            @Override
            protected Session getInternal() {
                return new TestLowSession();
            }
        };

        var session = new TsurugiSession(future, sessionOption) {
            @Override
            protected SqlClient newSqlClient(Session lowSession) {
                return new TestSqlClient(lowSession);
            }
        };
        if (modifier != null) {
            modifier.accept(session);
        }

        session.addChild(child);

        var sqlClient = (TestSqlClient) session.getLowSqlClient();
        var lowSession = (TestLowSession) sqlClient.getSession();
        lowSession.setExpectedCloseTimeout(TimeUnit.SECONDS.toNanos(1) - childSleep, TimeUnit.NANOSECONDS);
        lowSession.setThrowCloseTimeout(true);

        var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> session.close());
        assertEquals(IceaxeErrorCode.SESSION_CLOSE_TIMEOUT, e.getDiagnosticCode());

        assertTrue(future.isClosed());
        assertTrue(sqlClient.isClosed());
        assertTrue(lowSession.isClosed());
        assertTrue(child.closed);
    }
}
