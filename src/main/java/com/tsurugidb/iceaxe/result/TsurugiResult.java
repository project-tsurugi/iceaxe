package com.tsurugidb.iceaxe.result;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionIOException;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.TgTimeValue;
import com.tsurugidb.jogasaki.proto.SqlResponse.ResultOnly;

/**
 * Tsurugi Result for PreparedStatement
 */
public abstract class TsurugiResult implements Closeable {

    private final TsurugiTransaction ownerTransaction;
    private final IceaxeTimeout connectTimeout;
    private ResultOnly lowResultOnly;

    // internal
    public TsurugiResult(TsurugiTransaction transaction) {
        this.ownerTransaction = transaction;
        transaction.addChild(this);
        var info = transaction.getSessionInfo();
        this.connectTimeout = new IceaxeTimeout(info, TgTimeoutKey.RESULT_CONNECT);
    }

    /**
     * set connect-timeout
     * 
     * @param timeout time
     */
    public void setConnectTimeout(long time, TimeUnit unit) {
        setConnectTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set connect-timeout
     * 
     * @param timeout time
     */
    public void setConnectTimeout(TgTimeValue timeout) {
        connectTimeout.set(timeout);
    }

    protected synchronized final ResultOnly getLowResultOnly() throws IOException {
        if (this.lowResultOnly == null) {
            var lowResultOnlyFuture = getLowResultOnlyFuture();
            this.lowResultOnly = IceaxeIoUtil.getFromFuture(lowResultOnlyFuture, connectTimeout);
        }
        return this.lowResultOnly;
    }

    protected abstract FutureResponse<ResultOnly> getLowResultOnlyFuture() throws IOException;

    /**
     * get result status
     * <p>
     * TODO+++翻訳: DBサーバー側で処理が終わるまでは{@link TgResultStatus#RESULT_NOT_SET}を返す。<br>
     * DBサーバー側で処理が終わると{@link TgResultStatus#SUCCESS}または{@link TgResultStatus#ERROR}になる。
     * </p>
     * FIXME この説明で合っているか？
     * 
     * @return result status
     * @throws IOException
     */
    public TgResultStatus getResultStatus() throws IOException {
        var lowResultCase = getLowResultOnly().getResultCase();
        switch (lowResultCase) {
        case SUCCESS:
            return TgResultStatus.SUCCESS;
        case ERROR:
            return TgResultStatus.ERROR;
        case RESULT_NOT_SET:
            return TgResultStatus.RESULT_NOT_SET;
        default:
            throw new UnsupportedOperationException("unsupported status error. resultCase=" + lowResultCase);
        }
    }

    protected void checkResultStatus(boolean errorIfResultNotSet) throws IOException, TsurugiTransactionIOException {
        var lowResultOnly = getLowResultOnly();
        var lowResultCase = lowResultOnly.getResultCase();
        switch (lowResultCase) {
        case SUCCESS:
            break;
        case ERROR:
            throw new TsurugiTransactionIOException(lowResultOnly.getError());
        case RESULT_NOT_SET:
            if (errorIfResultNotSet) {
                throw new AssertionError(lowResultCase);
            }
            break;
        default:
            throw new AssertionError(lowResultCase);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            // FIXME resultCaseがRESULT_NOT_SETだったらどうすればよいか？
            checkResultStatus(true);
        } finally {
            ownerTransaction.removeChild(this);
        }
    }
}
