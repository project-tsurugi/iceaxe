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
package com.tsurugidb.iceaxe.exception;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.sql.exception.CcException;
import com.tsurugidb.tsubakuro.sql.exception.ConflictOnWritePreserveException;
import com.tsurugidb.tsubakuro.sql.exception.InactiveTransactionException;
import com.tsurugidb.tsubakuro.sql.exception.TargetNotFoundException;
import com.tsurugidb.tsubakuro.sql.exception.UniqueConstraintViolationException;

/**
 * Tsurugi Exception utility.
 */
public class TsurugiExceptionUtil {

    private static TsurugiExceptionUtil instance = new TsurugiExceptionUtil();

    /**
     * get default Exception utility.
     *
     * @return Exception utility
     */
    public static TsurugiExceptionUtil getInstance() {
        return instance;
    }

    /**
     * set default Exception utility.
     *
     * @param defaultExceptionUtil Exception utility
     */
    public static void setInstance(@Nonnull TsurugiExceptionUtil defaultExceptionUtil) {
        instance = Objects.requireNonNull(defaultExceptionUtil);
    }

    //

    // SQL_EXECUTION_EXCEPTION (SQL-02000)

    /**
     * whether unique constraint violation.
     *
     * @param e exception
     * @return {@code true} if unique constraint violation
     */
    public boolean isUniqueConstraintViolation(TsurugiDiagnosticCodeProvider e) {
        var lowException = e.findLowServerException().orElse(null);
        if (lowException != null) {
            if (lowException instanceof UniqueConstraintViolationException) { // SQL-02002
                return true;
            }
        }
        return false;
    }

    /**
     * whether target not found.
     *
     * @param e exception
     * @return {@code true} if target not found
     */
    public boolean isTargetNotFound(TsurugiDiagnosticCodeProvider e) {
        var lowException = e.findLowServerException().orElse(null);
        if (lowException != null) {
            if (lowException instanceof TargetNotFoundException) { // SQL-02014
                return true;
            }
        }
        return false;
    }

    /**
     * whether inactive transaction.
     *
     * @param e exception
     * @return {@code true} if inactive transaction
     */
    public boolean isInactiveTransaction(TsurugiDiagnosticCodeProvider e) {
        var lowException = e.findLowServerException().orElse(null);
        if (lowException != null) {
            if (lowException instanceof InactiveTransactionException) { // SQL-02025
                return true;
            }
        }
        return false;
    }

    // CC_EXCEPTION (SQL-04000)

    /**
     * whether serialization error.
     *
     * @param e exception
     * @return {@code true} if serialization error
     */
    public boolean isSerializationFailure(TsurugiDiagnosticCodeProvider e) {
        var lowException = e.findLowServerException().orElse(null);
        if (lowException != null) {
            if (lowException instanceof CcException) { // SQL-04000
                return true;
            }
        }
        return false;
    }

    /**
     * whether conflict on write preserve.
     *
     * @param e exception
     * @return {@code true} if conflict on write preserve
     */
    public boolean isConflictOnWritePreserve(TsurugiDiagnosticCodeProvider e) {
        var lowException = e.findLowServerException().orElse(null);
        if (lowException != null) {
            if (lowException instanceof ConflictOnWritePreserveException) { // SQL-04015
                return true;
            }
        }
        return false;
    }
}
