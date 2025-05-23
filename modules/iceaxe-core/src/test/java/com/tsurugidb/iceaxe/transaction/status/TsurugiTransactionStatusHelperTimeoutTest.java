/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.iceaxe.transaction.status;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeTimeoutIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.test.TestTsurugiSession;
import com.tsurugidb.iceaxe.test.low.TestFutureResponse;
import com.tsurugidb.iceaxe.test.low.TestLowTransaction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;
import com.tsurugidb.tsubakuro.sql.TransactionStatus.TransactionStatusWithMessage;

class TsurugiTransactionStatusHelperTimeoutTest {

    @Test
    void connectTimeout() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.TX_STATUS_CONNECT, 1, TimeUnit.SECONDS);

        var exceptionFuture = new TestFutureResponse<SqlServiceException>();
        exceptionFuture.setExpectedTimeout(1, TimeUnit.SECONDS);
        exceptionFuture.setThrowTimeout(true);

        var statusFuture = new TestFutureResponse<TransactionStatusWithMessage>();

        try (var session = new TestTsurugiSession(sessionOption)) {
            try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                var lowTx = (TestLowTransaction) transaction.getLowTransaction();
                lowTx.setTestSqlServiceExceptionFutureResponse(exceptionFuture);
                lowTx.setTestTransactionStatusFutureResponse(statusFuture);

                var target = new TsurugiTransactionStatusHelper();

                var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> target.getTransactionStatus(transaction));
                assertEquals(IceaxeErrorCode.TX_STATUS_CONNECT_TIMEOUT, e.getDiagnosticCode());
            }
        }

        assertTrue(exceptionFuture.isClosed());
    }

    @Test
    void connectTimeout2() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.TX_STATUS_CONNECT, 1, TimeUnit.SECONDS);

        var exceptionFuture = new TestFutureResponse<SqlServiceException>();

        var statusFuture = new TestFutureResponse<TransactionStatusWithMessage>();
        statusFuture.setExpectedTimeout(1, TimeUnit.SECONDS);
        statusFuture.setThrowTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                var lowTx = (TestLowTransaction) transaction.getLowTransaction();
                lowTx.setTestSqlServiceExceptionFutureResponse(exceptionFuture);
                lowTx.setTestTransactionStatusFutureResponse(statusFuture);

                var target = new TsurugiTransactionStatusHelper();

                var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> target.getTransactionStatus(transaction));
                assertEquals(IceaxeErrorCode.TX_STATUS_CONNECT_TIMEOUT, e.getDiagnosticCode());
            }
        }

        assertTrue(exceptionFuture.isClosed());
    }

    @Test
    void closeTimeout() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.TX_STATUS_CONNECT, 1, TimeUnit.SECONDS);

        var exceptionFuture = new TestFutureResponse<SqlServiceException>();
        exceptionFuture.setExpectedCloseTimeout(1, TimeUnit.SECONDS);
        exceptionFuture.setThrowCloseTimeout(true);

        var statusFuture = new TestFutureResponse<TransactionStatusWithMessage>();

        try (var session = new TestTsurugiSession(sessionOption)) {
            try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                var lowTx = (TestLowTransaction) transaction.getLowTransaction();
                lowTx.setTestSqlServiceExceptionFutureResponse(exceptionFuture);
                lowTx.setTestTransactionStatusFutureResponse(statusFuture);

                var target = new TsurugiTransactionStatusHelper();

                var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> target.getTransactionStatus(transaction));
                assertEquals(IceaxeErrorCode.TX_STATUS_CLOSE_TIMEOUT, e.getDiagnosticCode());
            }
        }

        assertTrue(exceptionFuture.isClosed());
    }

    @Test
    void closeTimeout2() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.TX_STATUS_CONNECT, 1, TimeUnit.SECONDS);

        var exceptionFuture = new TestFutureResponse<SqlServiceException>();

        var statusFuture = new TestFutureResponse<TransactionStatusWithMessage>();
        statusFuture.setExpectedCloseTimeout(1, TimeUnit.SECONDS);
        statusFuture.setThrowCloseTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                var lowTx = (TestLowTransaction) transaction.getLowTransaction();
                lowTx.setTestSqlServiceExceptionFutureResponse(exceptionFuture);
                lowTx.setTestTransactionStatusFutureResponse(statusFuture);

                var target = new TsurugiTransactionStatusHelper();

                var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> target.getTransactionStatus(transaction));
                assertEquals(IceaxeErrorCode.TX_STATUS_CLOSE_TIMEOUT, e.getDiagnosticCode());
            }
        }

        assertTrue(exceptionFuture.isClosed());
    }
}
