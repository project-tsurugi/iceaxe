package com.tsurugidb.iceaxe.transaction.status;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.test.TestTsurugiSession;
import com.tsurugidb.iceaxe.test.low.TestFutureResponse;
import com.tsurugidb.iceaxe.test.low.TestLowTransaction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;

class TsurugiTransactionStatusHelperTimeoutTest {

    @Test
    void connectTimeout() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.TX_STATUS_CONNECT, 1, TimeUnit.SECONDS);

        var future = new TestFutureResponse<SqlServiceException>();
        future.setExpectedTimeout(1, TimeUnit.SECONDS);
        future.setThrowTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                var lowTx = (TestLowTransaction) transaction.getLowTransaction();
                lowTx.setTestTransactionStatusFutureResponse(future);

                var target = new TsurugiTransactionStatusHelper();

                var e = assertThrowsExactly(IceaxeIOException.class, () -> target.getTransactionStatus(transaction));
                assertEquals(IceaxeErrorCode.TX_STATUS_CONNECT_TIMEOUT, e.getDiagnosticCode());
            }
        }

        assertTrue(future.isClosed());
    }

    @Test
    void closeTimeout() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.TX_STATUS_CONNECT, 1, TimeUnit.SECONDS);

        var future = new TestFutureResponse<SqlServiceException>();
        future.setExpectedCloseTimeout(1, TimeUnit.SECONDS);
        future.setThrowCloseTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                var lowTx = (TestLowTransaction) transaction.getLowTransaction();
                lowTx.setTestTransactionStatusFutureResponse(future);

                var target = new TsurugiTransactionStatusHelper();

                var e = assertThrowsExactly(IceaxeIOException.class, () -> target.getTransactionStatus(transaction));
                assertEquals(IceaxeErrorCode.TX_STATUS_CLOSE_TIMEOUT, e.getDiagnosticCode());
            }
        }

        assertTrue(future.isClosed());
    }
}
