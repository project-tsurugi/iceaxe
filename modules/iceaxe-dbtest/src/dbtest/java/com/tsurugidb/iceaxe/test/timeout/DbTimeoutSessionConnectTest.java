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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;

/**
 * session connect timeout test
 */
public class DbTimeoutSessionConnectTest extends DbTimetoutTest {

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
                sessionOption.setTimeout(TgTimeoutKey.SESSION_CONNECT, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    void timeoutSet() throws Exception {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySession(TsurugiSession session) {
                session.setConnectTimeout(1, TimeUnit.SECONDS);
            }
        });
    }

    @Override
    protected TsurugiConnector getTsurugiConnector(PipeServerThtread pipeServer) {
        pipeServer.setPipeWrite(false);
        return super.getTsurugiConnector(pipeServer);
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        try {
            session.getLowSqlClient();
        } catch (IceaxeIOException e) {
            assertEqualsCode(IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, e);
            assertFalse(session.isAlive());
            return;
        } finally {
            pipeServer.setPipeWrite(true);
        }
        fail("didn't time out");
    }

    @Override
    protected void handleWaitCompletionError(Exception e) throws IOException {
        if (e instanceof IceaxeIOException) {
            try {
                assertEqualsCode(IceaxeErrorCode.SESSION_LOW_ERROR, e);
                var c = e.getCause();
                assertEqualsCode(IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, c);
            } catch (Throwable t) {
                t.addSuppressed(e);
                throw t;
            }
        } else {
            super.handleWaitCompletionError(e);
        }
    }
}
