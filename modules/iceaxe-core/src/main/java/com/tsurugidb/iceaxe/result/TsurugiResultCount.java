package com.tsurugidb.iceaxe.result;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.TgTimeValue;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi Result Count for PreparedStatement
 */
@NotThreadSafe
public class TsurugiResultCount extends TsurugiResult {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiResultCount.class);

    private FutureResponse<Void> lowResultFuture;
    private final IceaxeTimeout checkTimeout;
    private final IceaxeTimeout closeTimeout;

    // internal
    public TsurugiResultCount(TsurugiTransaction transaction, FutureResponse<Void> lowResultFuture) {
        super(transaction);
        this.lowResultFuture = lowResultFuture;

        var info = transaction.getSessionInfo();
        this.checkTimeout = new IceaxeTimeout(info, TgTimeoutKey.RESULT_CHECK);
        this.closeTimeout = new IceaxeTimeout(info, TgTimeoutKey.RESULT_CLOSE);

        applyCloseTimeout();
    }

    private void applyCloseTimeout() {
        closeTimeout.apply(lowResultFuture);
    }

    /**
     * set check-timeout
     * 
     * @param timeout time
     */
    public void setCheckTimeout(long time, TimeUnit unit) {
        setCheckTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set check-timeout
     * 
     * @param timeout time
     */
    public void setCheckTimeout(TgTimeValue timeout) {
        checkTimeout.set(timeout);
    }

    /**
     * set close-timeout
     * 
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setCloseTimeout(long time, TimeUnit unit) {
        setCloseTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set close-timeout
     * 
     * @param timeout time
     */
    public void setCloseTimeout(TgTimeValue timeout) {
        closeTimeout.set(timeout);

        applyCloseTimeout();
    }

    protected final synchronized void checkLowResult() throws IOException, TsurugiTransactionException {
        if (this.lowResultFuture != null) {
            LOG.trace("lowResult get start");
            try {
                IceaxeIoUtil.getAndCloseFutureInTransaction(lowResultFuture, checkTimeout);
            } finally {
                this.lowResultFuture = null;
                applyCloseTimeout();
            }
            LOG.trace("lowResult get end");
        }
    }

    /**
     * get count
     * 
     * @return the row count for SQL Data Manipulation Language (DML) statements
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public int getUpdateCount() throws IOException, TsurugiTransactionException {
        checkLowResult();
        // FIXME 更新件数取得
//      throw new InternalError("not yet implements");
//      System.err.println("not yet implements TsurugiResultCount.getUpdateCount(), now always returns -1");
        return -1;
    }

    @Override
    public void close() throws IOException, TsurugiTransactionException {
        // checkLowResult()を一度も呼ばなくても、closeでステータスチェックされるはず

        LOG.trace("result close start");
        // not try-finally
        IceaxeIoUtil.closeInTransaction(lowResultFuture);
        super.close();
        LOG.trace("result close end");
    }
}
