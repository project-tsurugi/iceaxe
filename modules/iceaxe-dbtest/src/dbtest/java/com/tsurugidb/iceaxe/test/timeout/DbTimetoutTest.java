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
package com.tsurugidb.iceaxe.test.timeout;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPrepared;
import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;

public abstract class DbTimetoutTest extends DbTestTableTester {

    private static final int DEFULAT_TIMEOUT = 10; // seconds

    protected static class TimeoutModifier {
        public void modifySessionInfo(TgSessionOption sessionOption) {
            // do override
        }

        public void modifySession(TsurugiSession session) {
            // do override
        }

        public void modifyPs(TsurugiSqlPrepared<?> ps) {
            // do override
        }

        public void modifyTransaction(TsurugiTransaction transaction) {
            // do override
        }

        public void modifyQueryResult(TsurugiQueryResult<?> result) {
            // do override
        }

        public void modifyResultRecord(TsurugiResultRecord record) {
            // do override
        }

        public void modifyStatementResult(TsurugiStatementResult result) {
            // do override
        }
    }

    private final boolean closeSession;

    protected DbTimetoutTest() {
        this(true);
    }

    protected DbTimetoutTest(boolean closeSession) {
        this.closeSession = closeSession;
    }

    protected void testTimeout(TimeoutModifier modifier) throws IOException {
        DbTestConnector.assumeEndpointTcp();

        try (var pipeServer = new PipeServerThtread()) {
            pipeServer.start();
            var connector = getTsurugiConnector(pipeServer);
            var session = createSession(pipeServer, connector, modifier);

            try {
                AutoCloseable sessionCloser = () -> {
                    if (closeSession) {
                        long start = System.nanoTime();

                        session.close();

                        long end = System.nanoTime();
                        long duration = TimeUnit.NANOSECONDS.toSeconds(end - start);
                        if (duration >= DEFULAT_TIMEOUT * 1.2) {
                            throw new AssertionError(MessageFormat.format("Session.close() time exceeded {0} seconds", DEFULAT_TIMEOUT));
                        }
                    }
                };

                try (sessionCloser) {
                    try {
                        clientTask(pipeServer, session, modifier);
                    } finally {
                        pipeServer.setPipeWrite(true);
                    }
                } catch (IOException | RuntimeException | Error e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } finally {
                try {
                    var lowSession = (SessionImpl) session.getLowSession();
                    lowSession.waitForCompletion();
                } catch (Exception e) {
                    handleWaitCompletionError(e);
                }
            }
        }
    }

    protected TsurugiConnector getTsurugiConnector(PipeServerThtread pipeServer) {
        return pipeServer.getTsurugiConnector();
    }

    protected TsurugiSession createSession(PipeServerThtread pipeServer, TsurugiConnector connector, TimeoutModifier modifier) throws IOException {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, DEFULAT_TIMEOUT, TimeUnit.SECONDS);
        modifier.modifySessionInfo(sessionOption);

        TsurugiSession session = null;
        try {
            session = connector.createSession(sessionOption);
            DbTestConnector.addSession(session);
            modifier.modifySession(session);
        } catch (Throwable t) {
            if (session != null) {
                try {
                    session.close();
                } catch (Throwable e) {
                    t.addSuppressed(e);
                }
            }
            throw t;
        }
        assert session != null;
        return session;
    }

    protected abstract void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception;

    protected void handleWaitCompletionError(Exception e) throws IOException {
        LOG.error("lowSession.waitForCompletion() error", e);
    }
}
