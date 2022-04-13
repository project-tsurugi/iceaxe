package com.tsurugi.iceaxe.result;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;

import com.nautilus_technologies.tsubakuro.protos.ResponseProtos.ResultOnly;
import com.tsurugi.iceaxe.session.TgSessionInfo;
import com.tsurugi.iceaxe.statement.TsurugiPreparedStatement;
import com.tsurugi.iceaxe.util.IceaxeIoUtil;

/**
 * Tsurugi Result for PreparedStatement
 */
public class TsurugiResult implements Closeable {

    private final TsurugiPreparedStatement owerPreparedStatement;
    private Future<ResultOnly> lowResultOnlyFuture;
    private ResultOnly lowResultOnly;

    // internal
    public TsurugiResult(TsurugiPreparedStatement preparedStatement, Future<ResultOnly> lowResultOnlyFuture) {
        this.owerPreparedStatement = preparedStatement;
        this.lowResultOnlyFuture = lowResultOnlyFuture;
    }

    protected final TgSessionInfo getSessionInfo() {
        return owerPreparedStatement.getSessionInfo();
    }

    protected synchronized final ResultOnly getLowResultOnly() throws IOException {
        if (this.lowResultOnly == null) {
            var info = getSessionInfo();
            this.lowResultOnly = IceaxeIoUtil.getFromFuture(lowResultOnlyFuture, info);
            this.lowResultOnlyFuture = null;
        }
        return this.lowResultOnly;
    }

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

    @Override
    public void close() throws IOException {
        // not try-finally
        var status = getResultStatus();
        try {
            if (status != TgResultStatus.SUCCESS) {
                // TODO throw retry
            }
        } finally {
            owerPreparedStatement.removeChild(this);
        }
    }
}
