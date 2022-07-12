package com.tsurugidb.iceaxe.transaction;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.exception.SqlServiceCode;
import com.tsurugidb.jogasaki.proto.SqlResponse.Error;

/**
 * Tsurugi Transaction Exception
 */
@SuppressWarnings("serial")
public class TsurugiTransactionException extends Exception {

    // TODO 将来的にErrorは使用しなくなる想定
    private final Error lowError;
    private final ServerException lowServerException;

    // internal
    public TsurugiTransactionException(Error lowError) {
        super(lowError.getDetail());
        this.lowError = lowError;
        this.lowServerException = null;
    }

    // internal
    public TsurugiTransactionException(ServerException cause) {
        super(createMessage(cause), cause);
        this.lowError = null;
        this.lowServerException = cause;
    }

    private static String createMessage(ServerException e) {
        var code = e.getDiagnosticCode();
        return code.toString();
    }

    // TODO トランザクションがリトライ可能かどうか判定するのは別のクラスの方がいいかも
    public boolean isRetryable() {
        if (this.lowError != null) {
            var lowStatus = lowError.getStatus();
            switch (lowStatus) {
            case ERR_ABORTED_RETRYABLE:
                return true;
            default:
                return false;
            }
        }
        if (this.lowServerException != null) {
            var lowCode = lowServerException.getDiagnosticCode();
            if (lowCode == SqlServiceCode.ERR_ABORTED_RETRYABLE) {
                return true;
            }
            return false;
        }

        return false;
    }
}
