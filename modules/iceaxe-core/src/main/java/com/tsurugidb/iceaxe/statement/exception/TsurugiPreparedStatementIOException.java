package com.tsurugidb.iceaxe.statement.exception;

import java.io.IOException;

/**
 * Tsurugi PreparedStatement IOException
 */
@SuppressWarnings("serial")
public class TsurugiPreparedStatementIOException extends IOException {

    /** prepared statement already closed */
    public static final String MESSAGE_ALREADY_CLOSED = "prepared statement already closed";

    /**
     * Creates a new instance.
     *
     * @param message detail message
     */
    public TsurugiPreparedStatementIOException(String message) {
        super(message);
    }
}
