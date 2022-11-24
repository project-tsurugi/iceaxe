package com.tsurugidb.iceaxe.test.timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Timeout;
import org.opentest4j.AssertionFailedError;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;

/**
 * server stop (session) test
 */
public class DbServerStopSessionTest extends DbTimetoutTest {

    private static final int EXPECTED_TIMEOUT = 1;

    // サーバーが停止した場合に即座にエラーが返ることを確認するテスト
    @RepeatedTest(6)
    @Timeout(value = EXPECTED_TIMEOUT, unit = TimeUnit.SECONDS)
    void serverStop() throws IOException {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySessionInfo(TgSessionInfo info) {
                info.timeout(TgTimeoutKey.DEFAULT, EXPECTED_TIMEOUT + 1, TimeUnit.SECONDS);
            }
        });
    }

    @Override
    protected TsurugiSession createSession(PipeServerThtread pipeServer, TsurugiConnector connector, TimeoutModifier modifier) throws IOException {
        pipeServer.setPipeWrite(false);
        var session = super.createSession(pipeServer, connector, modifier);
        pipeServer.close(); // server stop
        return session;
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        var e = assertThrowsExactly(IOException.class, () -> {
            session.getLowSqlClient();
        });
        try {
            assertEquals("Server crashed", e.getMessage());
        } catch (AssertionFailedError t) {
            throw e;
        }
    }
}
