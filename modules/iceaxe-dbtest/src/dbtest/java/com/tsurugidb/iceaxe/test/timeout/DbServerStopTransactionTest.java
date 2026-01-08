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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.opentest4j.AssertionFailedError;

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * server stop (transaction) test
 */
public class DbServerStopTransactionTest extends DbTimetoutTest {

    private static final int EXPECTED_TIMEOUT = 1;

    // サーバーが停止した場合に即座にエラーが返ることを確認するテスト
    @Test
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
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        session.getLowSqlClient();

        pipeServer.setPipeWrite(false);
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            pipeServer.close(); // server stop (socket close)

            boolean ioe = false;
            Throwable save = null;
            try {
                transaction.getLowTransaction();
            } catch (IOException e) {
                ioe = true;
                try {
                    assertEquals("lost connection", e.getMessage());
                } catch (AssertionFailedError t) {
                    t.addSuppressed(e);
                    save = t;
                    throw t;
                }
                return;
            } catch (Throwable e) {
                save = e;
                throw e;
            } finally {
                pipeServer.setPipeWrite(true);

                try {
                    session.close();
                } catch (IOException e) {
                    if ("socket is already closed".equals(e.getMessage())) {
                        // pass
                    } else {
                        if (save != null) {
                            save.addSuppressed(e);
                        } else {
                            throw e;
                        }
                    }
                } catch (Throwable e) {
                    if (save != null) {
                        save.addSuppressed(e);
                    } else {
                        throw e;
                    }
                }

                assertTrue(ioe);
            }

            fail("didn't I/O error");
        }
    }
}
