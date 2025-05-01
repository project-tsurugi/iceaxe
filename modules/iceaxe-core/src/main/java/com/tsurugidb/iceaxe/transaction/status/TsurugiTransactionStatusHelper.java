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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.TsurugiExceptionUtil;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction.TgTxMethod;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.sql.TransactionStatus.TransactionStatusWithMessage;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi transaction status helper.
 */
public class TsurugiTransactionStatusHelper {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiTransactionStatusHelper.class);

    /**
     * get transaction status.
     *
     * @param transaction transaction
     * @return transaction status
     * @throws IOException          if an I/O error occurs while retrieving transaction status
     * @throws InterruptedException if interrupted while retrieving transaction status
     */
    public TgTxStatus getTransactionStatus(TsurugiTransaction transaction) throws IOException, InterruptedException {
        var sessionOption = transaction.getSession().getSessionOption();
        var connectTimeout = getConnectTimeout(sessionOption);

        var lowTx = transaction.getLowTransaction();
        LOG.trace("getTransactionStatus start. tx={}", transaction);
        var lowExceptionFuture = getLowSqlServiceException(lowTx);
        var lowTxStatusFuture = getLowTransactionStatus(lowTx);
        LOG.trace("getTransactionStatus started");
        return getTransactionStatus(transaction, lowExceptionFuture, lowTxStatusFuture, connectTimeout);
    }

    /**
     * get connect timeout.
     *
     * @param sessionOption session option
     * @return timeout
     */
    protected IceaxeTimeout getConnectTimeout(TgSessionOption sessionOption) {
        return new IceaxeTimeout(sessionOption, TgTimeoutKey.TX_STATUS_CONNECT);
    }

    /**
     * get close timeout.
     *
     * @param sessionOption session option
     * @return timeout
     */
    @Deprecated(since = "1.4.0")
    protected IceaxeTimeout getCloseTimeout(TgSessionOption sessionOption) {
        return new IceaxeTimeout(sessionOption, TgTimeoutKey.TX_STATUS_CLOSE);
    }

    /**
     * get low SQL service exception.
     *
     * @param lowTx low transaction
     * @return future of SQL service exception
     * @throws IOException if an I/O error occurs while retrieving transaction status
     */
    protected FutureResponse<SqlServiceException> getLowSqlServiceException(Transaction lowTx) throws IOException {
        return lowTx.getSqlServiceException();
    }

    /**
     * get low SQL service exception.
     *
     * @param lowTx low transaction
     * @return future of transaction status
     * @throws IOException if an I/O error occurs while retrieving transaction status
     * @since X.X.X
     */
    protected FutureResponse<TransactionStatusWithMessage> getLowTransactionStatus(Transaction lowTx) throws IOException {
        return lowTx.getStatus();
    }

    /**
     * get transaction status.
     *
     * @param transaction        transaction
     * @param lowExceptionFuture future of SQL service exception
     * @param lowTxStatusFuture  future of transaction status
     * @param connectTimeout     close timeout
     * @return transaction status
     * @throws IOException          if an I/O error occurs while retrieving transaction status
     * @throws InterruptedException if interrupted while retrieving transaction status
     */
    protected TgTxStatus getTransactionStatus(TsurugiTransaction transaction, FutureResponse<SqlServiceException> lowExceptionFuture, FutureResponse<TransactionStatusWithMessage> lowTxStatusFuture,
            IceaxeTimeout connectTimeout) throws IOException, InterruptedException {
        var lowException = IceaxeIoUtil.getAndCloseFuture(lowExceptionFuture, //
                connectTimeout, IceaxeErrorCode.TX_STATUS_CONNECT_TIMEOUT, //
                IceaxeErrorCode.TX_STATUS_CLOSE_TIMEOUT);
        TransactionStatusWithMessage lowTxStatus;
        try {
            lowTxStatus = IceaxeIoUtil.getAndCloseFuture(lowTxStatusFuture, //
                    connectTimeout, IceaxeErrorCode.TX_STATUS_CONNECT_TIMEOUT, //
                    IceaxeErrorCode.TX_STATUS_CLOSE_TIMEOUT);
        } catch (TsurugiIOException e) {
            if (TsurugiExceptionUtil.getInstance().isTransactionNotFound(e)) {
                lowTxStatus = null;
            } else {
                throw e;
            }
        }
        LOG.trace("getTransactionStatus end");

        var exception = newTransactionException(lowException);
        if (exception != null) {
            exception.setSql(transaction, null, null, null);
            exception.setTxMethod(TgTxMethod.GET_TRANSACTION_STATUS, 0);
        }

        return newTgTransactionStatus(exception, lowTxStatus);
    }

    /**
     * Creates a new transaction exception instance.
     *
     * @param lowException low exception
     * @return transaction exception
     */
    protected TsurugiTransactionException newTransactionException(SqlServiceException lowException) {
        if (lowException == null) {
            return null;
        }
        return new TsurugiTransactionException(lowException);
    }

    /**
     * Creates a new transaction status instance.
     *
     * @param exception   transaction exception
     * @param lowTxStatus transaction status
     * @return transaction status
     */
    protected TgTxStatus newTgTransactionStatus(TsurugiTransactionException exception, TransactionStatusWithMessage lowTxStatus) {
        return new TgTxStatus(exception, lowTxStatus);
    }
}
