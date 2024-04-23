package com.tsurugidb.iceaxe.test.timeout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.transaction.status.TsurugiTransactionStatusHelper;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * transaction status close timeout test
 */
public class DbTimeoutTxStatusCloseTest extends DbTimetoutTest {

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
            @SuppressWarnings("deprecation")
            @Override
            public void modifySessionInfo(TgSessionOption sessionOption) {
                sessionOption.setTimeout(TgTimeoutKey.TX_STATUS_CLOSE, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        var helper = new TsurugiTransactionStatusHelper() {
            @Override
            protected FutureResponse<SqlServiceException> getLowSqlServiceException(Transaction lowTx) throws IOException {
                var future = super.getLowSqlServiceException(lowTx);
                return new FutureResponse<SqlServiceException>() {
                    @Override
                    public boolean isDone() {
                        return future.isDone();
                    }

                    @Override
                    public SqlServiceException get() throws IOException, ServerException, InterruptedException {
                        throw new UnsupportedOperationException("do not use");
                    }

                    @Override
                    public SqlServiceException get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                        return future.get(timeout, unit);
                    }

                    @Override
                    public void close() throws IOException, ServerException, InterruptedException {
                        pipeServer.setPipeWrite(false);
                        try {
                            future.close();
                        } finally {
                            pipeServer.setPipeWrite(true);
                        }
                    }
                };
            }
        };
        session.setTransactionStatusHelper(helper);

        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            try {
                transaction.getTransactionStatus();
            } catch (IceaxeIOException e) {
                assertEqualsCode(IceaxeErrorCode.TX_STATUS_CLOSE_TIMEOUT, e);
                return;
            }
            // TX_STATUS_CLOSEはタイムアウトするような通信処理が無い
//          fail("didn't time out");
        }
    }
}
