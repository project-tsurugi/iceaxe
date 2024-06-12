package com.tsurugidb.iceaxe.exception;

import com.tsurugidb.iceaxe.util.IceaxeInternal;

/**
 * Iceaxe timeout IOException.
 *
 * @since X.X.X
 */
@SuppressWarnings("serial")
public class IceaxeTimeoutIOException extends IceaxeIOException {

    /**
     * Creates a new instance.
     *
     * @param code the diagnostic code
     */
    @IceaxeInternal
    public IceaxeTimeoutIOException(IceaxeErrorCode code) {
        super(code);
//      assert code.isTimeout();
    }

    /**
     * Creates a new instance.
     *
     * @param code  the diagnostic code
     * @param cause the cause
     */
    @IceaxeInternal
    public IceaxeTimeoutIOException(IceaxeErrorCode code, Throwable cause) {
        super(code, cause);
//      assert code.isTimeout();
    }
}
