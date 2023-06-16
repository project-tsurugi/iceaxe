package com.tsurugidb.iceaxe.transaction.manager.retry;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * default retry predicate
 */
@ThreadSafe
public class TsurugiDefaultRetryPredicate implements TsurugiTmRetryPredicate {

    private static TsurugiTmRetryPredicate instance = new TsurugiDefaultRetryPredicate();

    /**
     * get default retry predicate
     *
     * @return retry predicate
     */
    public static TsurugiTmRetryPredicate getInstance() {
        return instance;
    }

    /**
     * set default retry predicate
     *
     * @param defaultRetryPredicate retry predicate
     */
    public static void setInstance(@Nonnull TsurugiTmRetryPredicate defaultRetryPredicate) {
        instance = Objects.requireNonNull(defaultRetryPredicate);
    }

    @Override
    public final TgTmRetryInstruction apply(TsurugiTransaction transaction, TsurugiTransactionException e) {
        var instruction = test(transaction, e);
        Objects.requireNonNull(instruction);
        return instruction;
    }

    protected TgTmRetryInstruction test(TsurugiTransaction transaction, TsurugiTransactionException e) {
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

    protected static final void checkRetryInstruction(TgTmRetryInstruction instruction, String position, TsurugiTransactionException e) {
        if (instruction == null) {
            throw new IllegalStateException(position + " retryInstruction is null", e);
        }
    }

    protected TgTmRetryInstruction testOcc(TsurugiTransaction transaction, TsurugiTransactionException e) {
        if (isOccOnWp(e)) {
            return TgTmRetryInstruction.ofRetryableLtx("OCC ltx retry. " + e.getMessage());
        }

        return testCommon("OCC", transaction, e);
    }

    protected TgTmRetryInstruction testLtx(TsurugiTransaction transaction, TsurugiTransactionException e) {
        if (isOccOnWp(e)) {
            throw new IllegalStateException("illegal code. " + e.getMessage());
        }

        return testCommon("LTX", transaction, e);
    }

    protected TgTmRetryInstruction testRtx(TsurugiTransaction transaction, TsurugiTransactionException e) {
        if (isOccOnWp(e)) {
            throw new IllegalStateException("illegal code. " + e.getMessage());
        }

        return testCommon("RTX", transaction, e);
    }

    protected TgTmRetryInstruction testOther(TsurugiTransaction transaction, TsurugiTransactionException e) {
        return testCommon("OTHER", transaction, e);
    }

    protected TgTmRetryInstruction testCommon(String position, TsurugiTransaction transaction, TsurugiTransactionException e) {
        if (isRetryable(e)) {
            return TgTmRetryInstruction.ofRetryable(position + " retry. " + e.getMessage());
        }
        return TgTmRetryInstruction.ofNotRetryable(position + " not retry. " + e.getMessage());
    }

    //

    protected boolean isRetryable(TsurugiTransactionException e) {
        var code = e.getDiagnosticCode();
        if (code == SqlServiceCode.ERR_SERIALIZATION_FAILURE) {
            return true;
        }
        return false;
    }

    protected boolean isOccOnWp(TsurugiTransactionException e) {
        var code = e.getDiagnosticCode();
        if (code == SqlServiceCode.ERR_CONFLICT_ON_WRITE_PRESERVE) {
            return true;
        }
        if (code == SqlServiceCode.ERR_SERIALIZATION_FAILURE) {
            String message = e.getMessage();
            if (message.contains("shirakami response Status=ERR_CC")) {
                if (message.contains("reason_code:CC_OCC_WP_VERIFY")) {
                    return true;
                }
            }
        }
        return false;
    }
}
