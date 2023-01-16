package com.tsurugidb.iceaxe.test.timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.session.TsurugiSession;

/**
 * server stop (transaction) test
 */
public class DbServerStopTransactionTest extends DbTimetoutTest {

    private static final int EXPECTED_TIMEOUT = 1;

    // サーバーが停止した場合に即座にエラーが返ることを確認するテスト
    @Test
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
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        session.getLowSqlClient();

        pipeServer.setPipeWrite(false);
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            pipeServer.close(); // server stop

            try {
                transaction.getLowTransaction();
            } catch (IOException e) {
                assertEquals("Server crashed", e.getMessage());
                return;
            } finally {
                pipeServer.setPipeWrite(true);

                var e = assertThrowsExactly(IOException.class, () -> {
                    session.close();
                });
                assertEquals("socket is already closed", e.getMessage());
            }
            fail("didn't time out");
        }
    }
}
