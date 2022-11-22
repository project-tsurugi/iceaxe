package com.tsurugidb.iceaxe.test.timeout;

import java.io.Closeable;
import java.io.IOException;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.result.TsurugiResultCount;
import com.tsurugidb.iceaxe.result.TsurugiResultSet;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementWithLowPs;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

public abstract class DbTimetoutTest extends DbTestTableTester {

    protected static class TimeoutModifier {
        public void modifySessionInfo(TgSessionInfo info) {
            // do override
        }

        public void modifySession(TsurugiSession session) {
            // do override
        }

        public void modifyPs(TsurugiPreparedStatementWithLowPs<?> ps) {
            // do override
        }

        public void modifyTransaction(TsurugiTransaction transaction) {
            // do override
        }

        public void modifyResultSet(TsurugiResultSet<?> rs) {
            // do override
        }

        public void modifyResult(TsurugiResultCount result) {
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
            Closeable sessionCloser = () -> {
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
        }
    }

    protected TsurugiConnector getTsurugiConnector(PipeServerThtread pipeServer) {
        return pipeServer.getTsurugiConnector();
    }

    protected TsurugiSession createSession(PipeServerThtread pipeServer, TsurugiConnector connector, TimeoutModifier modifier) throws IOException {
        var info = TgSessionInfo.of();
        modifier.modifySessionInfo(info);

        TsurugiSession session = null;
        try {
            session = connector.createSession(info);
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
}
