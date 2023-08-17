package com.tsurugidb.iceaxe.transaction.status;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
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
        var lowFuture = getSqlServiceException(lowTx);
        LOG.trace("getTransactionStatus started");
        return getTransactionStatus(transaction, lowFuture);
    }

    protected FutureResponse<SqlServiceException> getSqlServiceException(Transaction lowTx) throws IOException {
        return lowTx.getSqlServiceException();
    }

    protected TgTransactionStatus getTransactionStatus(TsurugiTransaction transaction, FutureResponse<SqlServiceException> lowFuture) throws IOException, InterruptedException {
        try (var closeable = IceaxeIoUtil.closeable(lowFuture)) {

            var sessionOption = transaction.getSession().getSessionOption();
            var connectTimeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.TX_STATUS_CONNECT);
            var closeTimeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.TX_STATUS_CLOSE);
            closeTimeout.apply(lowFuture);

            var lowStatus = IceaxeIoUtil.getAndCloseFuture(lowFuture, connectTimeout);
            LOG.trace("getTransactionStatus end");

            return newTgTransactionStatus(lowStatus);
        }
    }

    protected TgTransactionStatus newTgTransactionStatus(SqlServiceException lowStatus) {
        return new TgTransactionStatus(lowStatus);
    }
}
