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
package com.tsurugidb.iceaxe.transaction.manager.retry;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.exception.TsurugiExceptionUtil;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;

/**
 * Tsurugi default retry predicate.
 */
@ThreadSafe
public class TsurugiDefaultRetryPredicate implements TsurugiTmRetryPredicate {

    private static TsurugiTmRetryPredicate instance = new TsurugiDefaultRetryPredicate();

    /**
     * get default retry predicate.
     *
     * @return retry predicate
     */
    public static TsurugiTmRetryPredicate getInstance() {
        return instance;
    }

    /**
     * set default retry predicate.
     *
     * @param defaultRetryPredicate retry predicate
     */
    public static void setInstance(@Nonnull TsurugiTmRetryPredicate defaultRetryPredicate) {
        instance = Objects.requireNonNull(defaultRetryPredicate);
    }

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
     * @return exception utility.
     */
    protected TsurugiExceptionUtil getExceptionUtil() {
        return this.exceptionUtil;
    }

    @Override
    public final TgTmRetryInstruction apply(TsurugiTransaction transaction, TsurugiTransactionException exception) throws IOException, InterruptedException {
        var instruction = test(transaction, exception);
        Objects.requireNonNull(instruction);
        return instruction;
    }

    /**
     * check retryable.
     *
     * @param transaction transaction
     * @param exception   transaction exception
     * @return retry instruction
     * @throws IOException          if an I/O error occurs while retrieving transaction status
     * @throws InterruptedException if interrupted while retrieving transaction status
     */
    protected TgTmRetryInstruction test(TsurugiTransaction transaction, TsurugiTransactionException exception) throws IOException, InterruptedException {
        TgTmRetryInstruction instruction;

        var txOption = transaction.getTransactionOption();
        var type = txOption.type();
        if (type == null) {
            type = TransactionType.TRANSACTION_TYPE_UNSPECIFIED;
        }
        switch (type) {
        case SHORT:
            instruction = testOcc(transaction, exception);
            checkRetryInstruction(instruction, "OCC", exception);
            break;
        case LONG:
            instruction = testLtx(transaction, exception);
            checkRetryInstruction(instruction, "LTX", exception);
            break;
        case READ_ONLY:
            instruction = testRtx(transaction, exception);
            checkRetryInstruction(instruction, "RTX", exception);
            break;
        default:
            instruction = testOther(transaction, exception);
            checkRetryInstruction(instruction, "OTHER", exception);
            break;
        }

        return instruction;
    }

    /**
     * check retryable.
     *
     * @param instruction retry instruction
     * @param position    check position
     * @param exception   transaction exception
     */
    protected static final void checkRetryInstruction(TgTmRetryInstruction instruction, String position, TsurugiTransactionException exception) {
        if (instruction == null) {
            throw new IllegalStateException(position + " retryInstruction is null", exception);
        }
    }

    /**
     * check retryable for OCC.
     *
     * @param transaction transaction
     * @param exception   transaction exception
     * @return retry instruction
     * @throws IOException          if an I/O error occurs while retrieving transaction status
     * @throws InterruptedException if interrupted while retrieving transaction status
     */
    protected TgTmRetryInstruction testOcc(TsurugiTransaction transaction, TsurugiTransactionException exception) throws IOException, InterruptedException {
        if (isConflictOnWritePreserve(exception)) {
            return TgTmRetryInstruction.ofRetryableLtx("OCC ltx retry. " + exception.getMessage());
        }

        return testCommon("OCC", transaction, exception);
    }

    /**
     * check retryable for LTX.
     *
     * @param transaction transaction
     * @param exception   transaction exception
     * @return retry instruction
     * @throws IOException          if an I/O error occurs while retrieving transaction status
     * @throws InterruptedException if interrupted while retrieving transaction status
     */
    protected TgTmRetryInstruction testLtx(TsurugiTransaction transaction, TsurugiTransactionException exception) throws IOException, InterruptedException {
        if (isConflictOnWritePreserve(exception)) {
            throw new IllegalStateException("illegal code. " + exception.getMessage());
        }

        return testCommon("LTX", transaction, exception);
    }

    /**
     * check retryable for RTX.
     *
     * @param transaction transaction
     * @param exception   transaction exception
     * @return retry instruction
     * @throws IOException          if an I/O error occurs while retrieving transaction status
     * @throws InterruptedException if interrupted while retrieving transaction status
     */
    protected TgTmRetryInstruction testRtx(TsurugiTransaction transaction, TsurugiTransactionException exception) throws IOException, InterruptedException {
        if (isConflictOnWritePreserve(exception)) {
            throw new IllegalStateException("illegal code. " + exception.getMessage());
        }

        return testCommon("RTX", transaction, exception);
    }

    /**
     * check retryable for other.
     *
     * @param transaction transaction
     * @param exception   transaction exception
     * @return retry instruction
     * @throws IOException          if an I/O error occurs while retrieving transaction status
     * @throws InterruptedException if interrupted while retrieving transaction status
     */
    protected TgTmRetryInstruction testOther(TsurugiTransaction transaction, TsurugiTransactionException exception) throws IOException, InterruptedException {
        return testCommon("OTHER", transaction, exception);
    }

    /**
     * check retryable for common.
     *
     * @param position    check position
     * @param transaction transaction
     * @param exception   transaction exception
     * @return retry instruction
     * @throws IOException          if an I/O error occurs while retrieving transaction status
     * @throws InterruptedException if interrupted while retrieving transaction status
     */
    protected TgTmRetryInstruction testCommon(String position, TsurugiTransaction transaction, TsurugiTransactionException exception) throws IOException, InterruptedException {
        if (isRetryable(exception)) {
            return TgTmRetryInstruction.ofRetryable(position + " retry. " + exception.getMessage());
        }
        if (isInactiveTransaction(exception)) {
            var status = transaction.getTransactionStatus();
            var statusException = status.getTransactionException();
            if (statusException != null) {
                if (isRetryable(statusException)) {
                    return TgTmRetryInstruction.ofRetryable(position + " retry. " + statusException.getMessage() + " with " + exception.getMessage());
                }
                return TgTmRetryInstruction.ofNotRetryable(position + " not retry. " + statusException.getMessage() + " with " + exception.getMessage());
            }
            return TgTmRetryInstruction.ofNotRetryable(position + " not retry. " + exception.getMessage() + ", status=null" /* + statusException */);
        }

        return TgTmRetryInstruction.ofNotRetryable(position + " not retry. " + exception.getMessage());
    }

    //

    /**
     * Whether to retry.
     *
     * @param exception transaction exception
     * @return {@code true} if retryable
     */
    protected boolean isRetryable(TsurugiTransactionException exception) {
        if (exceptionUtil.isSerializationFailure(exception)) {
            return true;
        }
        return false;
    }

    /**
     * Whether to conflict on write preserve.
     *
     * @param exception transaction exception
     * @return {@code true} if conflict on write preserve
     */
    protected boolean isConflictOnWritePreserve(TsurugiTransactionException exception) {
        if (exceptionUtil.isConflictOnWritePreserve(exception)) {
            return true;
        }
        if (exceptionUtil.isSerializationFailure(exception)) {
            // FIXME sub error code
            String message = exception.getMessage();
            if (message.contains("shirakami response Status=ERR_CC")) {
                if (message.contains("reason_code:CC_OCC_WP_VERIFY")) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Whether to inactive transaction.
     *
     * @param exception transaction exception
     * @return {@code true} if inactive transaction
     */
    protected boolean isInactiveTransaction(TsurugiTransactionException exception) {
        if (exceptionUtil.isInactiveTransaction(exception)) {
            return true;
        }
        return false;
    }
}
