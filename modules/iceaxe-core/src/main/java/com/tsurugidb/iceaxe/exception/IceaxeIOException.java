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

import com.tsurugidb.iceaxe.util.IceaxeInternal;

/**
 * Iceaxe IOException.
 *
 * @since 1.1.0
 */
@SuppressWarnings("serial")
public class IceaxeIOException extends TsurugiIOException {

    /**
     * Creates a new instance.
     *
     * @param code the diagnostic code
     */
    @IceaxeInternal
    public IceaxeIOException(IceaxeErrorCode code) {
        this(code, null);
    }

    /**
     * Creates a new instance.
     *
     * @param code  the diagnostic code
     * @param cause the cause
     */
    @IceaxeInternal
    public IceaxeIOException(IceaxeErrorCode code, Throwable cause) {
        super(code, createMessage(code, cause), cause);
    }

    private static String createMessage(IceaxeErrorCode code, Throwable cause) {
        if (cause != null) {
            String causeMessage = cause.getMessage();
            if (causeMessage != null && !causeMessage.isEmpty()) {
                return code.getMessage() + ": " + causeMessage;
            }
        }
        return code.getMessage();
    }

    @Override
    public IceaxeErrorCode getDiagnosticCode() {
        return (IceaxeErrorCode) super.getDiagnosticCode();
    }
}
