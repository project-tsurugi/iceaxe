package com.tsurugidb.iceaxe.exception;

import com.tsurugidb.iceaxe.metadata.TsurugiTableListHelper;
import com.tsurugidb.iceaxe.metadata.TsurugiTableMetadataHelper;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPrepared;
import com.tsurugidb.iceaxe.sql.explain.TsurugiExplainHelper;
import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.status.TsurugiTransactionStatusHelper;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.tsubakuro.exception.DiagnosticCode;

/**
 * Iceaxe diagnostic code.
 */
public enum IceaxeErrorCode implements DiagnosticCode {
    // common
    /**
     * close error.
     *
     * @see IceaxeIoUtil
     * @since X.X.X
     */
    CLOSE_ERROR(IceaxeErrorCodeBlock.COMMON + 1, "close error"),

    // session
    /**
     * timeout at {@link TsurugiSession#getLowSession()}.
     *
     * @since X.X.X
     */
    SESSION_CONNECT_TIMEOUT(IceaxeErrorCodeBlock.SESSION + 1, "session connect timeout"),
    /** low session error. */
    SESSION_LOW_ERROR(IceaxeErrorCodeBlock.SESSION + 2, "low session error"),
    /**
     * timeout at {@link TsurugiSession#close()}.
     *
     * @since X.X.X
     */
    SESSION_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.SESSION + 901, "session close timeout"),
    /** session already closed. */
    SESSION_ALREADY_CLOSED(IceaxeErrorCodeBlock.SESSION + 902, "session already closed"),

    // transaction
    /**
     * timeout at {@link TsurugiTransaction#getLowTransaction()}.
     *
     * @since X.X.X
     */
    TX_BEGIN_TIMEOUT(IceaxeErrorCodeBlock.TRANSACTION + 1, "transaction begin timeout"),
    /** low transaction error. */
    TX_LOW_ERROR(IceaxeErrorCodeBlock.TRANSACTION + 2, "low transaction error"),
    /**
     * timeout at {@link TsurugiTransactionStatusHelper#getTransactionStatus(TsurugiTransaction)} connect.
     *
     * @since X.X.X
     */
    TX_STATUS_CONNECT_TIMEOUT(IceaxeErrorCodeBlock.TRANSACTION + 101, "transaction getTransactionStatus connect timeout"),
    /**
     * timeout at {@link TsurugiTransactionStatusHelper#getTransactionStatus(TsurugiTransaction)} close.
     *
     * @since X.X.X
     */
    TX_STATUS_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.TRANSACTION + 191, "transaction getTransactionStatus close timeout"),
    /**
     * timeout at {@link TsurugiTransaction#commit(com.tsurugidb.iceaxe.transaction.TgCommitType)}.
     *
     * @since X.X.X
     */
    TX_COMMIT_TIMEOUT(IceaxeErrorCodeBlock.TRANSACTION + 701, "transaction commit timeout"),
    /**
     * timeout at {@link TsurugiTransaction#commit(com.tsurugidb.iceaxe.transaction.TgCommitType)} close.
     *
     * @since X.X.X
     */
    TX_COMMIT_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.TRANSACTION + 791, "transaction commit close timeout"),
    /**
     * timeout at {@link TsurugiTransaction#rollback()}.
     *
     * @since X.X.X
     */
    TX_ROLLBACK_TIMEOUT(IceaxeErrorCodeBlock.TRANSACTION + 801, "transaction rollback timeout"),
    /**
     * timeout at {@link TsurugiTransaction#rollback()} close.
     *
     * @since X.X.X
     */
    TX_ROLLBACK_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.TRANSACTION + 891, "transaction rollback close timeout"),
    /**
     * timeout at {@link TsurugiTransaction#close()}.
     *
     * @since X.X.X
     */
    TX_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.TRANSACTION + 901, "transaction close timeout"),
    /** transaction already closed. */
    TX_ALREADY_CLOSED(IceaxeErrorCodeBlock.TRANSACTION + 902, "transaction already closed"),

    // statement
    /**
     * timeout at {@link TsurugiSqlPrepared#getLowPreparedStatement()}.
     *
     * @since X.X.X
     */
    PS_CONNECT_TIMEOUT(IceaxeErrorCodeBlock.STATEMENT + 1, "prepared statement connect timeout"),
    /** low prepared statement error. */
    PS_LOW_ERROR(IceaxeErrorCodeBlock.STATEMENT + 2, "low prepared statement error"),
    /**
     * timeout at {@link TsurugiSqlPrepared#getLowPreparedStatement()}.
     *
     * @since X.X.X
     */
    PS_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.STATEMENT + 901, "prepared statement connect timeout"),
    /** prepared statement already closed. */
    PS_ALREADY_CLOSED(IceaxeErrorCodeBlock.STATEMENT + 902, "prepared statement already closed"),

    // result
    /**
     * timeout at {@link TsurugiQueryResult#getLowResultSet()}.
     *
     * @since X.X.X
     */
    RS_CONNECT_TIMEOUT(IceaxeErrorCodeBlock.RESULT + 101, "resultSet connect timeout"),
    /**
     * timeout at {@link TsurugiQueryResult#close()}.
     *
     * @since X.X.X
     */
    RS_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.RESULT + 191, "resultSet close timeout"),
    /**
     * timeout at {@link TsurugiStatementResult#checkLowResult()}.
     *
     * @since X.X.X
     */
    RESULT_CONNECT_TIMEOUT(IceaxeErrorCodeBlock.RESULT + 201, "executeResult connect timeout"),
    /**
     * timeout at {@link TsurugiStatementResult#close()}.
     *
     * @since X.X.X
     */
    RESULT_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.RESULT + 291, "executeResult close timeout"),

    // explain
    /**
     * timeout at {@link TsurugiExplainHelper} connect.
     *
     * @since X.X.X
     */
    EXPLAIN_CONNECT_TIMEOUT(IceaxeErrorCodeBlock.EXPLAIN + 1, "explain connect timeout"),
    /**
     * timeout at {@link TsurugiExplainHelper} close.
     *
     * @since X.X.X
     */
    EXPLAIN_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.EXPLAIN + 901, "explain close timeout"),

    // metadata
    /**
     * timeout at {@link TsurugiTableListHelper} connect.
     *
     * @since X.X.X
     */
    TABLE_LIST_CONNECT_TIMEOUT(IceaxeErrorCodeBlock.METADATA + 101, "getTableList connect timeout"),
    /**
     * timeout at {@link TsurugiTableListHelper} close.
     *
     * @since X.X.X
     */
    TABLE_LIST_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.METADATA + 191, "getTableList close timeout"),
    /**
     * timeout at {@link TsurugiTableMetadataHelper} connect.
     *
     * @since X.X.X
     */
    TABLE_METADATA_CONNECT_TIMEOUT(IceaxeErrorCodeBlock.METADATA + 201, "getTableMetadata connect timeout"),
    /**
     * timeout at {@link TsurugiTableMetadataHelper} close.
     *
     * @since X.X.X
     */
    TABLE_METADATA_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.METADATA + 291, "getTableMetadata close timeout"),

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
