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
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * transaction close timeout test
 */
public class DbTimeoutTransactionCloseTest extends DbTimetoutTest {

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
                sessionOption.setTimeout(TgTimeoutKey.TRANSACTION_CLOSE, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    void timeoutSet() throws Exception {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifyTransaction(TsurugiTransaction transaction) {
                transaction.setCloseTimeout(1, TimeUnit.SECONDS);
            }
        });
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        session.getLowSqlClient();

        var transaction = session.createTransaction(TgTxOption.ofOCC());
        modifier.modifyTransaction(transaction);

        transaction.getLowTransaction();

        pipeServer.setPipeWrite(false);
        try {
            transaction.close();
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
