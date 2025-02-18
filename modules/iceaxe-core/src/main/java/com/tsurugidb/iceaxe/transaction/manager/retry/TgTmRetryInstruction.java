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
package com.tsurugidb.iceaxe.transaction.manager.retry;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.exception.DiagnosticCode;

/**
 * Tsurugi TransactionManager retry instruction.
 */
public /* record */ final class TgTmRetryInstruction {

    /**
     * Creates a new instance.
     *
     * @param retryCode retry code
     * @param code      diagnostic code
     * @return retry instruction
     */
    public static TgTmRetryInstruction of(@Nonnull TgTmRetryCode retryCode, @Nonnull DiagnosticCode code) {
        String reasonMessage = getReasonMessage(code);
        return new TgTmRetryInstruction(retryCode, reasonMessage);
    }

    /**
     * Creates a new instance.
     *
     * @param retryCode     retry code
     * @param reasonMessage reason message
     * @return retry instruction
     */
    public static TgTmRetryInstruction of(@Nonnull TgTmRetryCode retryCode, @Nonnull String reasonMessage) {
        return new TgTmRetryInstruction(retryCode, reasonMessage);
    }

    /**
     * Creates a new instance.
     *
     * @param code diagnostic code
     * @return retry instruction
     */
    public static TgTmRetryInstruction ofRetryable(@Nonnull DiagnosticCode code) {
        String reasonMessage = getReasonMessage(code);
        return ofRetryable(reasonMessage);
    }

    /**
     * Creates a new instance.
     *
     * @param reasonMessage reason message
     * @return retry instruction
     */
    public static TgTmRetryInstruction ofRetryable(@Nonnull String reasonMessage) {
        return new TgTmRetryInstruction(TgTmRetryStandardCode.RETRYABLE, reasonMessage);
    }

    /**
     * Creates a new instance.
     *
     * @param reasonMessage reason message
     * @return retry instruction
     */
    public static TgTmRetryInstruction ofRetryableLtx(@Nonnull String reasonMessage) {
        return new TgTmRetryInstruction(TgTmRetryStandardCode.RETRYABLE_LTX, reasonMessage);
    }

    /**
     * Creates a new instance.
     *
     * @param code diagnostic code
     * @return retry instruction
     */
    public static TgTmRetryInstruction ofNotRetryable(@Nonnull DiagnosticCode code) {
        String reasonMessage = getReasonMessage(code);
        return ofNotRetryable(reasonMessage);
    }

    /**
     * Creates a new instance.
     *
     * @param reasonMessage reason message
     * @return retry instruction
     */
    public static TgTmRetryInstruction ofNotRetryable(@Nonnull String reasonMessage) {
        return new TgTmRetryInstruction(TgTmRetryStandardCode.NOT_RETRYABLE, reasonMessage);
    }

    private static String getReasonMessage(DiagnosticCode code) {
        return "diagnosticCode=" + code;
    }

    private final TgTmRetryCode retryCode;
    private final String reasonMessage;

    /**
     * Creates a new instance.
     *
     * @param retryCode     retry code
     * @param reasonMessage reason message
     */
    public TgTmRetryInstruction(@Nonnull TgTmRetryCode retryCode, @Nonnull String reasonMessage) {
        this.retryCode = Objects.requireNonNull(retryCode);
        this.reasonMessage = Objects.requireNonNull(reasonMessage);
    }

    /**
     * get retry code.
     *
     * @return retry code
     */
    public TgTmRetryCode retryCode() {
        return this.retryCode;
    }

    /**
     * get reason message.
     *
     * @return reason message
     */
    public String reasonMessage() {
        return this.reasonMessage;
    }

    /**
     * whether to retry.
     *
     * @return {@code true}: retryable
     */
    public boolean isRetryable() {
        return retryCode.isRetryable();
    }

    @Override
    public String toString() {
        return retryCode + "(" + reasonMessage + ")";
    }
}
