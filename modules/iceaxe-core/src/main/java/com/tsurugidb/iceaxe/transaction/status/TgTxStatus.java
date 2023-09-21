package com.tsurugidb.iceaxe.transaction.status;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.exception.TsurugiExceptionUtil;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.tsubakuro.exception.DiagnosticCode;

/**
 * Tsurugi transaction status.
 */
public class TgTxStatus {

    private final TsurugiTransactionException exception;
    private TsurugiExceptionUtil exceptionUtil = TsurugiExceptionUtil.getInstance();

    /**
     * set exception utility.
     *
     * @param execptionUtil exception utility
     */
    public void setExceptionUtil(@Nonnull TsurugiExceptionUtil execptionUtil) {
        this.exceptionUtil = Objects.requireNonNull(execptionUtil);
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
     * @param exception transaction exception
     */
    public TgTxStatus(@Nullable TsurugiTransactionException exception) {
        this.exception = exception;
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

    @Override
    public String toString() {
        return "TgTransactionStatus(" + exception + ")";
    }
}
