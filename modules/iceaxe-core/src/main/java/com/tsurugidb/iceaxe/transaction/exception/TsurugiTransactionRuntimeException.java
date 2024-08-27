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
package com.tsurugidb.iceaxe.transaction.exception;

import java.util.Optional;

import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;
import com.tsurugidb.tsubakuro.exception.DiagnosticCode;

/**
 * Tsurugi Transaction RuntimeException.
 */
@SuppressWarnings("serial")
public class TsurugiTransactionRuntimeException extends RuntimeException implements TsurugiDiagnosticCodeProvider {

    /**
     * Creates a new instance.
     *
     * @param cause TsurugiTransactionException
     */
    public TsurugiTransactionRuntimeException(TsurugiTransactionException cause) {
        super(cause.getMessage(), cause);
    }

    @Override
    public TsurugiTransactionException getCause() {
        return (TsurugiTransactionException) super.getCause();
    }

    @Override
    public DiagnosticCode getDiagnosticCode() {
        return getCause().getDiagnosticCode();
    }

    @Override
    public Optional<TsurugiTransactionException> findTransactionException() {
        return Optional.of(getCause());
    }
}
