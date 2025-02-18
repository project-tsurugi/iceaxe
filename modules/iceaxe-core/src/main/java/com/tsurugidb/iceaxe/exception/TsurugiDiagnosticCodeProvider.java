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

import java.util.Optional;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.sql.TsurugiSql;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction.TgTxMethod;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOptionSupplier;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * diagnostic code provider.
 */
public interface TsurugiDiagnosticCodeProvider {

    /**
     * get diagnostic code.
     *
     * @return diagnostic code (null if not found)
     */
    public @Nullable DiagnosticCode getDiagnosticCode();

    // utility

    /**
     * get diagnostic code provider.
     *
     * @param e Throwable
     * @return diagnostic code provider
     */
    public static Optional<TsurugiDiagnosticCodeProvider> findDiagnosticCodeProvider(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof TsurugiDiagnosticCodeProvider) {
                return Optional.of((TsurugiDiagnosticCodeProvider) t);
            }
            if (t instanceof ServerException) {
                var serverException = (ServerException) t;
                return Optional.of(new TsurugiDiagnosticCodeProvider() {
                    @Override
                    public DiagnosticCode getDiagnosticCode() {
                        return serverException.getDiagnosticCode();
                    }
                });
            }
        }
        return Optional.empty();
    }

    /**
     * create message.
     *
     * @param exception ServerException
     * @return message
     */
    public static @Nullable String createMessage(ServerException exception) {
        var code = exception.getDiagnosticCode();
        if (code != null) {
            return code.name() + ": " + exception.getMessage();
        } else {
            return exception.getMessage();
        }
    }

    /**
     * create message.
     *
     * @param code diagnostic code
     * @return message
     */
    public static @Nullable String createMessage(DiagnosticCode code) {
        if (code == null) {
            return null;
        }
        return code.toString();
    }

    // transaction information

    /**
     * get iceaxe transactionId.
     *
     * @return iceaxe transactionId
     */
    public default int getIceaxeTxId() {
        return findTransactionException().map(e -> e.getIceaxeTxId()).orElse(0);
    }

    /**
     * get iceaxe tm executeId.
     *
     * @return iceaxe tm executeId
     */
    public default int getIceaxeTmExecuteId() {
        return findTransactionException().map(e -> e.getIceaxeTmExecuteId()).orElse(0);
    }

    /**
     * get attempt number.
     *
     * @return attempt number
     * @see TgTmTxOptionSupplier#get(Object, int, com.tsurugidb.iceaxe.transaction.TsurugiTransaction, TsurugiTransactionException)
     */
    public default int getAttempt() {
        return findTransactionException().map(e -> e.getAttempt()).orElse(0);
    }

    /**
     * get transaction option.
     *
     * @return transaction option
     */
    public default TgTxOption getTransactionOption() {
        return findTransactionException().map(e -> e.getTransactionOption()).orElse(null);
    }

    /**
     * get transaction id.
     *
     * @return transaction id
     */
    public default @Nullable String getTransactionId() {
        return findTransactionException().map(e -> e.getTransactionId()).orElse(null);
    }

    // transaction execute information

    /**
     * get transaction method.
     *
     * @return transaction method
     */
    public default @Nullable TgTxMethod getTxMethod() {
        return findTransactionException().map(e -> e.getTxMethod()).orElse(null);
    }

    /**
     * get iceaxe tx executeId.
     *
     * @return iceaxe tx executeId
     */
    public default int getIceaxeTxExecuteId() {
        return findTransactionException().map(e -> e.getIceaxeTxExecuteId()).orElse(0);
    }

    /**
     * get SQL definition.
     *
     * @return SQL definition
     */
    public default @Nullable TsurugiSql getSqlDefinition() {
        return findTransactionException().map(e -> e.getSqlDefinition()).orElse(null);
    }

    /**
     * get SQL parameter.
     *
     * @return SQL parameter
     */
    public default @Nullable Object getSqlParameter() {
        return findTransactionException().map(e -> e.getSqlParameter()).orElse(null);
    }

    /**
     * get iceaxe SQL executeId.
     *
     * @return iceaxe SQL executeId
     */
    public default int getIceaxeSqlExecuteId() {
        return findTransactionException().map(e -> e.getIceaxeSqlExecuteId()).orElse(0);
    }

    /**
     * get transaction exception.
     *
     * @return transaction exception
     */
    public default Optional<TsurugiTransactionException> findTransactionException() {
        for (var t = (Throwable) this; t != null; t = t.getCause()) {
            if (t instanceof TsurugiTransactionException) {
                return Optional.of((TsurugiTransactionException) t);
            }
        }
        return Optional.empty();
    }

    /**
     * get low ServerException.
     *
     * @return ServerException
     */
    public default Optional<ServerException> findLowServerException() {
        for (var t = (Throwable) this; t != null; t = t.getCause()) {
            if (t instanceof ServerException) {
                return Optional.of((ServerException) t);
            }
        }
        return Optional.empty();
    }
}
