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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Timeout;
import org.opentest4j.AssertionFailedError;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;

/**
 * server stop (session) test
 */
public class DbServerStopSessionTest extends DbTimetoutTest {

    private static final int EXPECTED_TIMEOUT = 1;

    // サーバーが停止した場合に即座にエラーが返ることを確認するテスト
    @RepeatedTest(6)
    @Timeout(value = EXPECTED_TIMEOUT, unit = TimeUnit.SECONDS)
    void serverStop() throws Exception {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySessionInfo(TgSessionOption sessionOption) {
                sessionOption.setTimeout(TgTimeoutKey.DEFAULT, EXPECTED_TIMEOUT + 1, TimeUnit.SECONDS);
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
            assertEquals("lost connection", e.getMessage());
        } catch (AssertionFailedError t) {
            t.addSuppressed(e);
            throw t;
        }
    }

    @Override
    protected void handleWaitCompletionError(Exception e) throws IOException {
        if (e instanceof IceaxeIOException) {
            try {
                assertEqualsCode(IceaxeErrorCode.SESSION_LOW_ERROR, e);
                var c = e.getCause();
                assertEquals("lost connection", c.getMessage());
            } catch (Throwable t) {
                t.addSuppressed(e);
                throw t;
            }
        } else {
            super.handleWaitCompletionError(e);
        }
    }
}
