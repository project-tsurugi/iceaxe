package com.tsurugidb.iceaxe.util;

/**
 * Tsurugi RuntimeException for InterruptedException
 */
@SuppressWarnings("serial")
public class InterruptedRuntimeException extends RuntimeException {

    /**
     * Creates a new instance.
     *
     * @param cause the cause
     */
    public InterruptedRuntimeException(InterruptedException cause) {
        super(cause);
    }

    @Override
    public InterruptedException getCause() {
        return (InterruptedException) super.getCause();
    }
}
