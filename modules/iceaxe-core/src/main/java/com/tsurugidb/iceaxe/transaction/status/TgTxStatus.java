/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.iceaxe.transaction.status;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.exception.TsurugiExceptionUtil;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.sql.TransactionStatus;
import com.tsurugidb.tsubakuro.sql.TransactionStatus.TransactionStatusWithMessage;

/**
 * Tsurugi transaction status.
 */
public class TgTxStatus {

    private final TsurugiTransactionException exception;
    private final TransactionStatusWithMessage lowTxStatus;
    private TsurugiExceptionUtil exceptionUtil = TsurugiExceptionUtil.getInstance();

    /**
     * set exception utility.
     *
     * @param exceptionUtil exception utility
     */
    public void setExceptionUtil(@Nonnull TsurugiExceptionUtil exceptionUtil) {
        this.exceptionUtil = Objects.requireNonNull(exceptionUtil);
    }

    /**
     * get exception utility.
     *
     * @return exception utility
     */
    protected TsurugiExceptionUtil getExceptionUtil() {
        return this.exceptionUtil;
    }

    /**
     * Creates a new instance.
     *
     * @param exception   transaction exception
     * @param lowTxStatus transaction status
     */
    public TgTxStatus(@Nullable TsurugiTransactionException exception, @Nullable TransactionStatusWithMessage lowTxStatus) {
        this.exception = exception;
        this.lowTxStatus = lowTxStatus;
    }

    /**
     * Whether the status is normal.
     *
     * @return {@code true} if status is normal
     */
    public boolean isNormal() {
        if (this.exception == null) {
            return true;
        }
        return false;
    }

    /**
     * Whether the status is error.
     *
     * @return {@code true} if status is error
     */
    public boolean isError() {
        return !isNormal();
    }

    /**
     * get diagnostic code.
     *
     * @return diagnostic code. {@code null} if exception is null
     */
    public @Nullable DiagnosticCode getDiagnosticCode() {
        if (this.exception == null) {
            return null;
        }
        return exception.getDiagnosticCode();
    }

    /**
     * get exception.
     *
     * @return exception
     */
    @IceaxeInternal
    public @Nullable TsurugiTransactionException getTransactionException() {
        return this.exception;
    }

    /**
     * Whether the transaction is found.
     *
     * @return {@code true} if transaction is found
     * @since X.X.X
     */
    public boolean isTransactionFound() {
        return this.lowTxStatus != null;
    }

    /**
     * get transaction status.
     *
     * @return transaction status
     * @since X.X.X
     */
    public @Nullable TransactionStatus getLowTransactionStatus() {
        if (this.lowTxStatus == null) {
            return null;
        }
        return lowTxStatus.getStatus();
    }

    /**
     * get transaction status message.
     *
     * @return message
     * @since X.X.X
     */
    public @Nullable String getTransactionStatusMessage() {
        if (this.lowTxStatus == null) {
            return null;
        }
        return lowTxStatus.getMessage();
    }

    @Override
    public String toString() {
        return "TgTransactionStatus(" + exception + ", " + getLowTransactionStatus() + ")";
    }
}
