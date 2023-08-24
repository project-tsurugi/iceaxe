package com.tsurugidb.iceaxe.transaction.status;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * Tsurugi transaction status helper
 */
public class TsurugiTransactionStatusHelper {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiTransactionStatusHelper.class);

    /**
     * get transaction status.
     *
     * @param transaction transaction
     * @return transaction status
     * @throws IOException
     * @throws InterruptedException
     */
    public TgTransactionStatus getTransactionStatus(TsurugiTransaction transaction) throws IOException, InterruptedException {
        var lowTx = transaction.getLowTransaction();
        LOG.trace("getTransactionStatus start. tx={}", transaction);
        var lowFuture = getLowSqlServiceException(lowTx);
        LOG.trace("getTransactionStatus started");
        return getTransactionStatus(transaction, lowFuture);
    }

    protected FutureResponse<SqlServiceException> getLowSqlServiceException(Transaction lowTx) throws IOException {
        return lowTx.getSqlServiceException();
    }

    protected TgTransactionStatus getTransactionStatus(TsurugiTransaction transaction, FutureResponse<SqlServiceException> lowFuture) throws IOException, InterruptedException {
        try (var closeable = IceaxeIoUtil.closeable(lowFuture)) {

            var sessionOption = transaction.getSession().getSessionOption();
            var connectTimeout = getConnectTimeout(sessionOption);
            var closeTimeout = getCloseTimeout(sessionOption);
            closeTimeout.apply(lowFuture);

            var lowStatus = IceaxeIoUtil.getAndCloseFuture(lowFuture, connectTimeout);
            LOG.trace("getTransactionStatus end");

            var exception = newTransactionException(lowStatus);
            if (exception != null) {
                exception.setSql(transaction, null, null, null);
                exception.setTxMethod(TgTxMethod.GET_TRANSACTION_STATUS, 0);
            }
            return newTgTransactionStatus(exception);
        }
    }

    protected IceaxeTimeout getConnectTimeout(TgSessionOption sessionOption) {
        return new IceaxeTimeout(sessionOption, TgTimeoutKey.TX_STATUS_CONNECT);
    }

    protected IceaxeTimeout getCloseTimeout(TgSessionOption sessionOption) {
        return new IceaxeTimeout(sessionOption, TgTimeoutKey.TX_STATUS_CLOSE);
    }

    protected TsurugiTransactionException newTransactionException(SqlServiceException lowException) {
        if (lowException == null) {
            return null;
        }
        return new TsurugiTransactionException(lowException);
    }

    protected TgTransactionStatus newTgTransactionStatus(TsurugiTransactionException exception) {
        return new TgTransactionStatus(exception);
    }
}