package com.tsurugidb.iceaxe.test.timeout;

import java.io.IOException;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.result.TsurugiResultCount;
import com.tsurugidb.iceaxe.result.TsurugiResultSet;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementWithLowPs;
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

    protected void testTimeout(TimeoutModifier modifier) throws IOException {
        try (var pipeServer = new PipeServerThtread()) {
            pipeServer.start();

            var connector = getTsurugiConnector(pipeServer);

            var info = TgSessionInfo.of();
            modifier.modifySessionInfo(info);
            try (var session = connector.createSession(info)) {
                modifier.modifySession(session);

                try {
                    clientTask(pipeServer, session, modifier);
                } finally {
                    pipeServer.setSend(true);
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

    protected abstract void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception;
}
