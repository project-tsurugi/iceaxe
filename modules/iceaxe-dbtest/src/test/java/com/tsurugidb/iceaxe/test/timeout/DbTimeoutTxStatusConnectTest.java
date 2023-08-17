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
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.transaction.status.TsurugiTransactionStatusHelper;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * transaction status connect timeout test
 */
public class DbTimeoutTxStatusConnectTest extends DbTimetoutTest {

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
                sessionOption.setTimeout(TgTimeoutKey.TX_STATUS_CONNECT, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        var helper = new TsurugiTransactionStatusHelper() {
            @Override
            protected FutureResponse<SqlServiceException> getLowSqlServiceException(Transaction lowTx) throws IOException {
                pipeServer.setPipeWrite(false);
                return super.getLowSqlServiceException(lowTx);
            }
        };
        session.setTransactionStatusHelper(helper);

        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            try {
                transaction.getTransactionStatus();
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
}
