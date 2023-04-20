package com.tsurugidb.iceaxe.test.timeout;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;

/**
 * session close timeout test
 */
public class DbTimeoutSessionCloseTest extends DbTimetoutTest {

    public DbTimeoutSessionCloseTest() {
        super(false);
    }

    @Test
    void timeoutDefault() throws Exception {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySessionInfo(TgSessionOption sessionOption) {
                sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    void timeoutSpecified() throws Exception {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySessionInfo(TgSessionOption sessionOption) {
                sessionOption.setTimeout(TgTimeoutKey.SESSION_CLOSE, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    void timeoutSet() throws Exception {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySession(TsurugiSession session) {
                session.setCloseTimeout(1, TimeUnit.SECONDS);
            }
        });
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        session.getLowSqlClient();

        pipeServer.setPipeWrite(false);
        try {
            session.close();
        } catch (IOException e) {
            assertInstanceOf(TimeoutException.class, e.getCause());
            LOG.trace("timeout success");
            return;
        } finally {
            pipeServer.setPipeWrite(true);
        }
        fail("didn't time out");
    }
}
