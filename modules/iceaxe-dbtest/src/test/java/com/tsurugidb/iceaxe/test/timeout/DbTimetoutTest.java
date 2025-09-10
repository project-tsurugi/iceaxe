package com.tsurugidb.iceaxe.test.timeout;

import java.io.IOException;
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
                        session.close();
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
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 10, TimeUnit.SECONDS);
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
