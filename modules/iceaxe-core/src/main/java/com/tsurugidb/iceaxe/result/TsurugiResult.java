package com.tsurugidb.iceaxe.result;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.TgTimeValue;

/**
 * Tsurugi Result for PreparedStatement
 */
@NotThreadSafe
public abstract class TsurugiResult implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiResult.class);

    private final TsurugiTransaction ownerTransaction;
    private final IceaxeTimeout checkTimeout;
    private final IceaxeTimeout closeTimeout;
    private boolean resultChecked = false;

    // internal
    public TsurugiResult(TsurugiTransaction transaction) {
        this.ownerTransaction = transaction;
        transaction.addChild(this);

        var info = transaction.getSessionInfo();
        this.checkTimeout = new IceaxeTimeout(info, TgTimeoutKey.RESULT_CHECK);
        this.closeTimeout = new IceaxeTimeout(info, TgTimeoutKey.RESULT_CLOSE);

        applyCloseTimeout();
    }

    private void applyCloseTimeout() {
//      closeTimeout.apply(lowResultFuture);
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
        if (!this.resultChecked) {
            this.resultChecked = true;
            var lowResultFuture = getLowResultFutureOnce();
            LOG.trace("lowResult get start");
            IceaxeIoUtil.checkAndCloseTransactionFuture(lowResultFuture, checkTimeout, closeTimeout);
            LOG.trace("lowResult get end");
        }
    }

    protected abstract FutureResponse<Void> getLowResultFutureOnce() throws IOException;

    @Override
    @OverridingMethodsMustInvokeSuper
    public void close() throws IOException {
        ownerTransaction.removeChild(this);
    }
}
