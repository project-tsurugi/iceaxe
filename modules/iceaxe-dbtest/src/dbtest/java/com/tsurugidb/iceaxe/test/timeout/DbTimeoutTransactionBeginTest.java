package com.tsurugidb.iceaxe.test.timeout;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * transaction begin timeout test
 */
public class DbTimeoutTransactionBeginTest extends DbTimetoutTest {

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
                sessionOption.setTimeout(TgTimeoutKey.TRANSACTION_BEGIN, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    void timeoutSet() throws Exception {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifyTransaction(TsurugiTransaction transaction) {
                transaction.setBeginTimeout(1, TimeUnit.SECONDS);
            }
        });
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        session.getLowSqlClient();

        pipeServer.setPipeWrite(false);
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            modifier.modifyTransaction(transaction);

            try {
                transaction.getLowTransaction();
            } catch (IceaxeIOException e) {
                assertEqualsCode(IceaxeErrorCode.TX_BEGIN_TIMEOUT, e);
                assertFalse(transaction.available());
                return;
            } finally {
                pipeServer.setPipeWrite(true);
            }
            fail("didn't time out");
        } finally {
            pipeServer.setPipeWrite(true);
        }
    }
}
