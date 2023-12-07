package com.tsurugidb.iceaxe.exception;

import com.tsurugidb.iceaxe.util.IceaxeInternal;

/**
 * Iceaxe IOException.
 *
 * @since 1.1.0
 */
@SuppressWarnings("serial")
public class IceaxeIOException extends TsurugiIOException {

    /**
     * Creates a new instance.
     *
     * @param code the diagnostic code
     */
    @IceaxeInternal
    public IceaxeIOException(IceaxeErrorCode code) {
        this(code, null);
    }

    /**
     * Creates a new instance.
     *
     * @param code  the diagnostic code
     * @param cause the cause
     */
    @IceaxeInternal
    public IceaxeIOException(IceaxeErrorCode code, Throwable cause) {
        super(code, createMessage(code, cause), cause);
    }

    private static String createMessage(IceaxeErrorCode code, Throwable cause) {
        if (cause != null) {
            String causeMessage = cause.getMessage();
            if (causeMessage != null && !causeMessage.isEmpty()) {
                return code.getMessage() + ": " + causeMessage;
            }
        }
        return code.getMessage();
    }

    @Override
    public IceaxeErrorCode getDiagnosticCode() {
        return (IceaxeErrorCode) super.getDiagnosticCode();
    }
}
