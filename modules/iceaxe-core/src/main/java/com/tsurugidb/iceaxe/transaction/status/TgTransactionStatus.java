package com.tsurugidb.iceaxe.transaction.status;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;

/**
 * Tsurugi transaction status
 */
public class TgTransactionStatus {

    private final SqlServiceException lowException;

    /**
     * Creates a new instance.
     *
     * @param lowException low exception
     */
    public TgTransactionStatus(@Nullable SqlServiceException lowException) {
        this.lowException = lowException;
    }

    /**
     * Whether the status is normal.
     *
     * @return {@code true} if status is normal
     */
    public boolean isNormal() {
        return this.lowException == null;
    }

    /**
     * Whether the status is error.
     *
     * @return {@code true} if status is error
     */
    public boolean isError() {
        return this.lowException != null;
    }

    /**
     * get diagnostic code.
     *
     * @return diagnostic code. {@code null} if status is normal
     */
    public @Nullable SqlServiceCode getDiagnosticCode() {
        if (this.lowException == null) {
            return null;
        }
        return lowException.getDiagnosticCode();
    }

    /**
     * get low exception.
     *
     * @return low exception
     */
    @IceaxeInternal
    public @Nullable SqlServiceException getLowSqlServiceException() {
        return this.lowException;
    }
}
