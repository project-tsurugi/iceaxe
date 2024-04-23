package com.tsurugidb.iceaxe.transaction.status;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction.TgTxMethod;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;
import com.tsurugidb.tsubakuro.sql.Transaction;
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
        var lowFuture = getLowSqlServiceException(lowTx);
        LOG.trace("getTransactionStatus started");
        return getTransactionStatus(transaction, lowFuture, connectTimeout);
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
    @Deprecated(since = "X.X.X")
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
     * get transaction status.
     *
     * @param transaction    transaction
     * @param lowFuture      future of SQL service exception
     * @param connectTimeout close timeout
     * @return transaction status
     * @throws IOException          if an I/O error occurs while retrieving transaction status
     * @throws InterruptedException if interrupted while retrieving transaction status
     */
    protected TgTxStatus getTransactionStatus(TsurugiTransaction transaction, FutureResponse<SqlServiceException> lowFuture, IceaxeTimeout connectTimeout) throws IOException, InterruptedException {
        var lowStatus = IceaxeIoUtil.getAndCloseFuture(lowFuture, //
                connectTimeout, IceaxeErrorCode.TX_STATUS_CONNECT_TIMEOUT, //
                IceaxeErrorCode.TX_STATUS_CLOSE_TIMEOUT);
        LOG.trace("getTransactionStatus end");

        var exception = newTransactionException(lowStatus);
        if (exception != null) {
            exception.setSql(transaction, null, null, null);
            exception.setTxMethod(TgTxMethod.GET_TRANSACTION_STATUS, 0);
        }
        return newTgTransactionStatus(exception);
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
     * @param exception transaction exception
     * @return transaction status
     */
    protected TgTxStatus newTgTransactionStatus(TsurugiTransactionException exception) {
        return new TgTxStatus(exception);
    }
}
