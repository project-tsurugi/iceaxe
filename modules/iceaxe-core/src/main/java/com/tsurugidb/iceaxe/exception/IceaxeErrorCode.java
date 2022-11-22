package com.tsurugidb.iceaxe.exception;

import com.tsurugidb.tsubakuro.exception.DiagnosticCode;

/**
 * Iceaxe diagnostic code.
 */
public enum IceaxeErrorCode implements DiagnosticCode {
    /** session already closed */
    SESSION_ALREADY_CLOSED(IceaxeErrorCodeBlock.SESSION + 1, "session already closed"),
    /** low session error */
    SESSION_LOW_ERROR(IceaxeErrorCodeBlock.SESSION + 2, "low session error"),

    /** transaction already closed */
    TX_ALREADY_CLOSED(IceaxeErrorCodeBlock.TRANSACTION + 1, "transaction already closed"),
    /** low transaction error */
    TX_LOW_ERROR(IceaxeErrorCodeBlock.TRANSACTION + 2, "low transaction error"),

    /** prepared statement already closed */
    PS_ALREADY_CLOSED(IceaxeErrorCodeBlock.STATEMENT + 1, "prepared statement already closed"),
    /** low prepared statement error */
    PS_LOW_ERROR(IceaxeErrorCodeBlock.STATEMENT + 2, "low prepared statement error"),

    //
    ;

    private final int codeNumber;
    private final String message;

    private IceaxeErrorCode(int codeNumber, String message) {
        this.codeNumber = codeNumber;
        this.message = message;
    }

    /**
     * Structured code prefix of server diagnostics.
     */
    public static final String PREFIX_STRUCTURED_CODE = "ICEAXE"; //$NON-NLS-1$

    @Override
    public String getStructuredCode() {
        return String.format("%s-%05d", PREFIX_STRUCTURED_CODE, getCodeNumber()); //$NON-NLS-1$
    }

    @Override
    public int getCodeNumber() {
        return this.codeNumber;
    }

    /**
     * Returns message.
     *
     * @return message
     */
    public String getMessage() {
        return this.message;
    }

    @Override
    public String toString() {
        return String.format("%s %s", getStructuredCode(), getMessage());
    }
}
