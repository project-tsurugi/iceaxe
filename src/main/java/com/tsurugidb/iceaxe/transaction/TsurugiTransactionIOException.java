package com.tsurugidb.iceaxe.transaction;

import java.io.IOException;

import com.tsurugidb.jogasaki.proto.SqlResponse.Error;

/**
 * Tsurugi Transaction IOException
 */
@SuppressWarnings("serial")
public class TsurugiTransactionIOException extends IOException {

    private final Error lowError;

    // internal
    public TsurugiTransactionIOException(Error lowError) {
        super(lowError.getDetail());
        this.lowError = lowError;
    }

    // internal
    public TsurugiTransactionIOException(String message) {
        super(message);
        this.lowError = null;
    }

    // internal
    public TsurugiTransactionIOException(Throwable cause) {
        super(cause);
        this.lowError = null;
    }

    public boolean isRetryable() {
        if (this.lowError == null) {
            return false;
        }

        var lowStatus = lowError.getStatus();
        switch (lowStatus) {
        case ERR_ABORTED_RETRYABLE:
            return true;
        default:
            return false;
        }
    }
}
