package com.tsurugidb.iceaxe.test.timeout;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.session.TgSessionShutdownType;
import com.tsurugidb.iceaxe.session.TsurugiSession;

/**
 * session shutdown timeout test
 */
public class DbTimeoutSessionShutdownTest extends DbTimetoutTest {

    @Test
    void timeoutSpecified() throws Exception {
        testTimeout(new TimeoutModifier());
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        pipeServer.setPipeWrite(false);
        try {
            session.shutdown(TgSessionShutdownType.GRACEFUL, 1, TimeUnit.SECONDS);
        } catch (IceaxeIOException e) {
            assertEqualsCode(IceaxeErrorCode.SESSION_SHUTDOWN_TIMEOUT, e);
            assertFalse(session.isAlive());
            return;
        } finally {
            pipeServer.setPipeWrite(true);
        }
        fail("didn't time out");
    }
}
