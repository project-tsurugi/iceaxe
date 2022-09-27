package com.tsurugidb.iceaxe.test.timeout;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;

/**
 * session connect timeout test
 */
@Disabled // want to remove Disabled
public class DbTimeoutSessionConnectTest extends DbTimetoutTest {

    @Test
    void timeoutDefault() throws IOException {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySessionInfo(TgSessionInfo info) {
                info.timeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    void timeoutSpecified() throws IOException {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySessionInfo(TgSessionInfo info) {
                info.timeout(TgTimeoutKey.SESSION_CONNECT, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    void timeoutSet() throws IOException {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySession(TsurugiSession session) {
                session.setConnectTimeout(1, TimeUnit.SECONDS);
            }
        });
    }

    @Override
    protected TsurugiConnector getTsurugiConnector(PipeServerThtread pipeServer) {
        pipeServer.setSend(false);
        return super.getTsurugiConnector(pipeServer);
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        try {
            session.getLowSqlClient();
        } catch (IOException e) {
            assertInstanceOf(TimeoutException.class, e.getCause());
            return;
        } finally {
            pipeServer.setSend(true);
        }
        fail("didn't time out");
    }
}
