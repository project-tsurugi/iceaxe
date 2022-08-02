package com.tsurugidb.iceaxe.result;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.annotation.concurrent.NotThreadSafe;

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

    private final TsurugiTransaction ownerTransaction;
    private final IceaxeTimeout checkTimeout;
    private boolean getResult = false;

    // internal
    public TsurugiResult(TsurugiTransaction transaction) {
        this.ownerTransaction = transaction;
        transaction.addChild(this);
        var info = transaction.getSessionInfo();
        this.checkTimeout = new IceaxeTimeout(info, TgTimeoutKey.RESULT_CHECK);
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

    protected final synchronized void checkLowResult() throws IOException, TsurugiTransactionException {
        if (!this.getResult) {
            this.getResult = true;
            var lowResultFuture = getLowResultFuture();
            IceaxeIoUtil.getFromTransactionFuture(lowResultFuture, checkTimeout);
        }
    }

    protected abstract FutureResponse<Void> getLowResultFuture() throws IOException;

    @Override
    @OverridingMethodsMustInvokeSuper
    public void close() throws IOException {
        ownerTransaction.removeChild(this);
    }
}
