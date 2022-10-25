package com.tsurugidb.iceaxe.exception;

import com.tsurugidb.tsubakuro.exception.DiagnosticCode;

/**
 * Iceaxe diagnostic code.
 */
public enum IceaxeErrorCode implements DiagnosticCode {
    /** transaction already closed */
    TX_ALREADY_CLOSED(IceaxeErrorCodeBlock.TRANSACTION + 1, "transaction already closed"),

    /** prepared statement already closed */
    PS_ALREADY_CLOSED(IceaxeErrorCodeBlock.STATEMENT + 1, "prepared statement already closed"),

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
