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
     * @param execptionUtil exception utility
     */
    public void setExceptionUtil(@Nonnull TsurugiExceptionUtil execptionUtil) {
        this.exceptionUtil = Objects.requireNonNull(execptionUtil);
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
    public final TgTmRetryInstruction apply(TsurugiTransaction transaction, TsurugiTransactionException e) throws IOException, InterruptedException {
        var instruction = test(transaction, e);
        Objects.requireNonNull(instruction);
        return instruction;
    }

    /**
     * check retryable.
     *
     * @param transaction transaction
     * @param e           transaction exception
     * @return retry instruction
     * @throws IOException
     * @throws InterruptedException
     */
    protected TgTmRetryInstruction test(TsurugiTransaction transaction, TsurugiTransactionException e) throws IOException, InterruptedException {
        TgTmRetryInstruction instruction;

        var txOption = transaction.getTransactionOption();
        var type = txOption.type();
        if (type == null) {
            type = TransactionType.TRANSACTION_TYPE_UNSPECIFIED;
        }
        switch (type) {
        case SHORT:
            instruction = testOcc(transaction, e);
            checkRetryInstruction(instruction, "OCC", e);
            break;
        case LONG:
            instruction = testLtx(transaction, e);
            checkRetryInstruction(instruction, "LTX", e);
            break;
        case READ_ONLY:
            instruction = testRtx(transaction, e);
            checkRetryInstruction(instruction, "RTX", e);
            break;
        default:
            instruction = testOther(transaction, e);
            checkRetryInstruction(instruction, "OTHER", e);
            break;
        }

        return instruction;
    }

    /**
     * check retryable.
     *
     * @param instruction retry instruction
     * @param position    check position
     * @param e           transaction exception
     */
    protected static final void checkRetryInstruction(TgTmRetryInstruction instruction, String position, TsurugiTransactionException e) {
        if (instruction == null) {
            throw new IllegalStateException(position + " retryInstruction is null", e);
        }
    }

    /**
     * check retryable for OCC.
     *
     * @param transaction transaction
     * @param e           transaction exception
     * @return retry instruction
     * @throws IOException
     * @throws InterruptedException
     */
    protected TgTmRetryInstruction testOcc(TsurugiTransaction transaction, TsurugiTransactionException e) throws IOException, InterruptedException {
        if (isConflictOnWritePreserve(e)) {
            return TgTmRetryInstruction.ofRetryableLtx("OCC ltx retry. " + e.getMessage());
        }

        return testCommon("OCC", transaction, e);
    }

    /**
     * check retryable for LTX.
     *
     * @param transaction transaction
     * @param e           transaction exception
     * @return retry instruction
     * @throws IOException
     * @throws InterruptedException
     */
    protected TgTmRetryInstruction testLtx(TsurugiTransaction transaction, TsurugiTransactionException e) throws IOException, InterruptedException {
        if (isConflictOnWritePreserve(e)) {
            throw new IllegalStateException("illegal code. " + e.getMessage());
        }

        return testCommon("LTX", transaction, e);
    }

    /**
     * check retryable for RTX.
     *
     * @param transaction transaction
     * @param e           transaction exception
     * @return retry instruction
     * @throws IOException
     * @throws InterruptedException
     */
    protected TgTmRetryInstruction testRtx(TsurugiTransaction transaction, TsurugiTransactionException e) throws IOException, InterruptedException {
        if (isConflictOnWritePreserve(e)) {
            throw new IllegalStateException("illegal code. " + e.getMessage());
        }

        return testCommon("RTX", transaction, e);
    }

    /**
     * check retryable for other.
     *
     * @param transaction transaction
     * @param e           transaction exception
     * @return retry instruction
     * @throws IOException
     * @throws InterruptedException
     */
    protected TgTmRetryInstruction testOther(TsurugiTransaction transaction, TsurugiTransactionException e) throws IOException, InterruptedException {
        return testCommon("OTHER", transaction, e);
    }

    /**
     * check retryable for common.
     *
     * @param position    check position
     * @param transaction transaction
     * @param e           transaction exception
     * @return retry instruction
     * @throws IOException
     * @throws InterruptedException
     */
    protected TgTmRetryInstruction testCommon(String position, TsurugiTransaction transaction, TsurugiTransactionException e) throws IOException, InterruptedException {
        if (isRetryable(e)) {
            return TgTmRetryInstruction.ofRetryable(position + " retry. " + e.getMessage());
        }
        if (isInactiveTransaction(e)) {
            var status = transaction.getTransactionStatus();
            var statusException = status.getTransactionException();
            if (statusException != null) {
                if (isRetryable(statusException)) {
                    return TgTmRetryInstruction.ofRetryable(position + " retry. " + statusException.getMessage() + " with " + e.getMessage());
                }
                return TgTmRetryInstruction.ofNotRetryable(position + " not retry. " + statusException.getMessage() + " with " + e.getMessage());
            }
        }

        return TgTmRetryInstruction.ofNotRetryable(position + " not retry. " + e.getMessage());
    }

    //

    /**
     * Whether to retry.
     *
     * @param e transaction exception
     * @return {@code true} if retryable
     */
    protected boolean isRetryable(TsurugiTransactionException e) {
        if (exceptionUtil.isSerializationFailure(e)) {
            return true;
        }
        return false;
    }

    /**
     * Whether to conflict on write preserve.
     *
     * @param e transaction exception
     * @return {@code true} if conflict on write preserve
     */
    protected boolean isConflictOnWritePreserve(TsurugiTransactionException e) {
        if (exceptionUtil.isConflictOnWritePreserve(e)) {
            return true;
        }
        if (exceptionUtil.isSerializationFailure(e)) {
            // FIXME sub error code
            String message = e.getMessage();
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
     * @param e transaction exception
     * @return {@code true} if inactive transaction
     */
    protected boolean isInactiveTransaction(TsurugiTransactionException e) {
        if (exceptionUtil.isInactiveTransaction(e)) {
            return true;
        }
        return false;
    }
}
