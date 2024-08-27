/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.iceaxe.exception;

import com.tsurugidb.iceaxe.metadata.TsurugiTableListHelper;
import com.tsurugidb.iceaxe.metadata.TsurugiTableMetadataHelper;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPrepared;
import com.tsurugidb.iceaxe.sql.explain.TsurugiExplainHelper;
import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.status.TsurugiTransactionStatusHelper;
import com.tsurugidb.tsubakuro.exception.DiagnosticCode;

/**
 * Iceaxe diagnostic code.
 */
public enum IceaxeErrorCode implements DiagnosticCode {

    // session
    /**
     * {@link TsurugiSession#getLowSession()} timeout.
     *
     * @since 1.3.0
     */
    SESSION_CONNECT_TIMEOUT(IceaxeErrorCodeBlock.SESSION + 1, "session connect timeout"),
    /** low session error. */
    SESSION_LOW_ERROR(IceaxeErrorCodeBlock.SESSION + 2, "low session error"),
    /**
     * {@link TsurugiSession#shutdown(com.tsurugidb.iceaxe.session.TgSessionShutdownType, long, java.util.concurrent.TimeUnit)} timeout.
     *
     * @since 1.4.0
     */
    SESSION_SHUTDOWN_TIMEOUT(IceaxeErrorCodeBlock.SESSION + 801, "session shutdown timeout"),
    /**
     * {@link TsurugiSession#shutdown(com.tsurugidb.iceaxe.session.TgSessionShutdownType, long, java.util.concurrent.TimeUnit)} close timeout.
     *
     * @since 1.4.0
     */
    SESSION_SHUTDOWN_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.SESSION + 802, "session shutdown close timeout"),
    /**
     * session child resource close error at {@link TsurugiSession#close()}.
     *
     * @since 1.3.0
     */
    SESSION_CHILD_CLOSE_ERROR(IceaxeErrorCodeBlock.SESSION + 901, "session child resource close error"),
    /**
     * {@link TsurugiSession#close()} timeout.
     *
     * @since 1.3.0
     */
    SESSION_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.SESSION + 902, "session close timeout"),
    /**
     * {@link TsurugiSession#close()} error.
     *
     * @since 1.3.0
     */
    SESSION_CLOSE_ERROR(IceaxeErrorCodeBlock.SESSION + 903, "session close error"),
    /** session already closed. */
    SESSION_ALREADY_CLOSED(IceaxeErrorCodeBlock.SESSION + 909, "session already closed"),

    // transaction manager
    /**
     * {@link TsurugiTransactionManager} rollback error.
     *
     * @since 1.3.0
     */
    TM_ROLLBACK_ERROR(IceaxeErrorCodeBlock.TRANSACTION_MANAGER + 801, "transactionManager rollback error"),

    // transaction
    /**
     * {@link TsurugiTransaction#getLowTransaction()} timeout.
     *
     * @since 1.3.0
     */
    TX_BEGIN_TIMEOUT(IceaxeErrorCodeBlock.TRANSACTION + 1, "transaction begin timeout"),
    /** low transaction error. */
    TX_LOW_ERROR(IceaxeErrorCodeBlock.TRANSACTION + 2, "low transaction error"),
    /**
     * {@link TsurugiTransactionStatusHelper#getTransactionStatus(TsurugiTransaction)} connect timeout.
     *
     * @since 1.3.0
     */
    TX_STATUS_CONNECT_TIMEOUT(IceaxeErrorCodeBlock.TRANSACTION + 101, "transaction getTransactionStatus connect timeout"),
    /**
     * {@link TsurugiTransactionStatusHelper#getTransactionStatus(TsurugiTransaction)} close timeout.
     *
     * @since 1.3.0
     */
    TX_STATUS_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.TRANSACTION + 191, "transaction getTransactionStatus close timeout"),
    /**
     * {@link TsurugiTransactionStatusHelper#getTransactionStatus(TsurugiTransaction)} close error.
     *
     * @since 1.3.0
     */
    @Deprecated(since = "1.4.0")
    TX_STATUS_CLOSE_ERROR(IceaxeErrorCodeBlock.TRANSACTION + 192, "transaction getTransactionStatus close error"),
    /**
     * transaction child resource close error at {@link TsurugiTransaction#commit(com.tsurugidb.iceaxe.transaction.TgCommitType)}.
     *
     * @since 1.3.0
     */
    TX_COMMIT_CHILD_CLOSE_ERROR(IceaxeErrorCodeBlock.TRANSACTION + 701, "transaction child resource close error before commit"),
    /**
     * {@link TsurugiTransaction#commit(com.tsurugidb.iceaxe.transaction.TgCommitType)} timeout.
     *
     * @since 1.3.0
     */
    TX_COMMIT_TIMEOUT(IceaxeErrorCodeBlock.TRANSACTION + 711, "transaction commit timeout"),
    /**
     * {@link TsurugiTransaction#commit(com.tsurugidb.iceaxe.transaction.TgCommitType)} close timeout.
     *
     * @since 1.3.0
     */
    TX_COMMIT_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.TRANSACTION + 791, "transaction commit close timeout"),
    /**
     * transaction child resource close error at {@link TsurugiTransaction#rollback()}.
     *
     * @since 1.3.0
     */
    TX_ROLLBACK_CHILD_CLOSE_ERROR(IceaxeErrorCodeBlock.TRANSACTION + 801, "transaction child resource close error before rollback"),
    /**
     * {@link TsurugiTransaction#rollback()} timeout.
     *
     * @since 1.3.0
     */
    TX_ROLLBACK_TIMEOUT(IceaxeErrorCodeBlock.TRANSACTION + 811, "transaction rollback timeout"),
    /**
     * {@link TsurugiTransaction#rollback()} close timeout.
     *
     * @since 1.3.0
     */
    TX_ROLLBACK_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.TRANSACTION + 891, "transaction rollback close timeout"),
    /**
     * session child resource close error at {@link TsurugiTransaction#close()}.
     *
     * @since 1.3.0
     */
    TX_CHILD_CLOSE_ERROR(IceaxeErrorCodeBlock.TRANSACTION + 901, "transaction child resource close error"),
    /**
     * {@link TsurugiTransaction#close()} timeout.
     *
     * @since 1.3.0
     */
    TX_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.TRANSACTION + 902, "transaction close timeout"),
    /**
     * {@link TsurugiTransaction#close()} error.
     *
     * @since 1.3.0
     */
    TX_CLOSE_ERROR(IceaxeErrorCodeBlock.TRANSACTION + 903, "transaction close error"),
    /** transaction already closed. */
    TX_ALREADY_CLOSED(IceaxeErrorCodeBlock.TRANSACTION + 909, "transaction already closed"),

    // statement
    /**
     * {@link TsurugiSqlPrepared#getLowPreparedStatement()} timeout.
     *
     * @since 1.3.0
     */
    PS_CONNECT_TIMEOUT(IceaxeErrorCodeBlock.STATEMENT + 1, "prepared statement connect timeout"),
    /** low prepared statement error. */
    PS_LOW_ERROR(IceaxeErrorCodeBlock.STATEMENT + 2, "low prepared statement error"),
    /**
     * {@link TsurugiSqlPrepared#close()} timeout.
     *
     * @since 1.3.0
     */
    PS_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.STATEMENT + 901, "prepared statement close timeout"),
    /**
     * {@link TsurugiSqlPrepared#close()} error.
     *
     * @since 1.3.0
     */
    PS_CLOSE_ERROR(IceaxeErrorCodeBlock.STATEMENT + 902, "prepared statement close error"),
    /** prepared statement already closed. */
    PS_ALREADY_CLOSED(IceaxeErrorCodeBlock.STATEMENT + 909, "prepared statement already closed"),

    // result
    /**
     * {@link TsurugiQueryResult#getLowResultSet()} timeout.
     *
     * @since 1.3.0
     */
    RS_CONNECT_TIMEOUT(IceaxeErrorCodeBlock.RESULT + 101, "resultSet connect timeout"),
    /**
     * {@link TsurugiQueryResult#close()} timeout.
     *
     * @since 1.3.0
     */
    RS_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.RESULT + 191, "resultSet close timeout"),
    /**
     * {@link TsurugiQueryResult#close()} error.
     *
     * @since 1.3.0
     */
    RS_CLOSE_ERROR(IceaxeErrorCodeBlock.RESULT + 192, "resultSet close error"),
    /**
     * {@link TsurugiStatementResult#checkLowResult()} timeout.
     *
     * @since 1.3.0
     */
    RESULT_CONNECT_TIMEOUT(IceaxeErrorCodeBlock.RESULT + 201, "executeResult connect timeout"),
    /**
     * {@link TsurugiStatementResult#close()} timeout.
     *
     * @since 1.3.0
     */
    RESULT_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.RESULT + 291, "executeResult close timeout"),
    /**
     * {@link TsurugiStatementResult#close()} error.
     *
     * @since 1.3.0
     */
    RESULT_CLOSE_ERROR(IceaxeErrorCodeBlock.RESULT + 292, "executeResult close error"),

    // explain
    /**
     * {@link TsurugiExplainHelper} connect timeout.
     *
     * @since 1.3.0
     */
    EXPLAIN_CONNECT_TIMEOUT(IceaxeErrorCodeBlock.EXPLAIN + 1, "explain connect timeout"),
    /**
     * {@link TsurugiExplainHelper} close timeout.
     *
     * @since 1.3.0
     */
    EXPLAIN_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.EXPLAIN + 901, "explain close timeout"),
    /**
     * {@link TsurugiExplainHelper} close error.
     *
     * @since 1.3.0
     */
    @Deprecated(since = "1.4.0")
    EXPLAIN_CLOSE_ERROR(IceaxeErrorCodeBlock.EXPLAIN + 902, "explain close error"),

    // metadata
    /**
     * {@link TsurugiTableListHelper} connect timeout.
     *
     * @since 1.3.0
     */
    TABLE_LIST_CONNECT_TIMEOUT(IceaxeErrorCodeBlock.METADATA + 101, "getTableList connect timeout"),
    /**
     * {@link TsurugiTableListHelper} close timeout.
     *
     * @since 1.3.0
     */
    TABLE_LIST_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.METADATA + 191, "getTableList close timeout"),
    /**
     * {@link TsurugiTableListHelper} close error.
     *
     * @since 1.3.0
     */
    @Deprecated(since = "1.4.0")
    TABLE_LIST_CLOSE_ERROR(IceaxeErrorCodeBlock.METADATA + 192, "getTableList close error"),
    /**
     * {@link TsurugiTableMetadataHelper} connect timeout.
     *
     * @since 1.3.0
     */
    TABLE_METADATA_CONNECT_TIMEOUT(IceaxeErrorCodeBlock.METADATA + 201, "getTableMetadata connect timeout"),
    /**
     * {@link TsurugiTableMetadataHelper} close timeout.
     *
     * @since 1.3.0
     */
    TABLE_METADATA_CLOSE_TIMEOUT(IceaxeErrorCodeBlock.METADATA + 291, "getTableMetadata close timeout"),
    /**
     * {@link TsurugiTableMetadataHelper} close error.
     *
     * @since 1.3.0
     */
    @Deprecated(since = "1.4.0")
    TABLE_METADATA_CLOSE_ERROR(IceaxeErrorCodeBlock.METADATA + 292, "getTableMetadata close error"),

    //
    ;

    private final int codeNumber;
    private final String message;
    private final boolean isTimeout;

    private IceaxeErrorCode(int codeNumber, String message) {
        this.codeNumber = codeNumber;
        this.message = message;
        this.isTimeout = name().endsWith("TIMEOUT");
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

    /**
     * Returns timeout.
     *
     * @return {@code true}: timeout
     * @since 1.4.0
     */
    public boolean isTimeout() {
        return this.isTimeout;
    }

    @Override
    public String toString() {
        return String.format("%s %s", getStructuredCode(), getMessage());
    }
}
