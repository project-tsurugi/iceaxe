package com.tsurugidb.iceaxe.transaction.manager.retry;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
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
    public TgTmRetryInstruction apply(TsurugiTransaction transaction, TsurugiDiagnosticCodeProvider e) {
        TgTmRetryInstruction reason = testCommon(transaction, e);
        if (reason != null) {
            return reason;
        }

        var txOption = transaction.getTransactionOption();
        var type = txOption.type();
        if (type == null) {
            return testOther(transaction, e);
        }
        switch (type) {
        case SHORT:
            return testOcc(transaction, e);
        case LONG:
            return testLtx(transaction, e);
        case READ_ONLY:
            return testRtx(transaction, e);
        default:
            return testOther(transaction, e);
        }
    }

    protected @Nullable TgTmRetryInstruction testCommon(TsurugiTransaction transaction, TsurugiDiagnosticCodeProvider e) {
        var lowCode = e.getDiagnosticCode();
        if (lowCode == SqlServiceCode.ERR_ABORTED_RETRYABLE) {
            return TgTmRetryInstruction.ofRetryable(lowCode);
        }
        return null;
    }

    protected TgTmRetryInstruction testOcc(TsurugiTransaction transaction, TsurugiDiagnosticCodeProvider e) {
        var lowCode = e.getDiagnosticCode();
        if (lowCode == SqlServiceCode.ERR_CONFLICT_ON_WRITE_PRESERVE) {
            return TgTmRetryInstruction.of(TgTmRetryStandardCode.RETRYABLE_LTX, "OCC diagnosticCode=" + lowCode);
        }
        return TgTmRetryInstruction.ofNotRetryable("OCC diagnosticCode=" + lowCode);
    }

    protected TgTmRetryInstruction testLtx(TsurugiTransaction transaction, TsurugiDiagnosticCodeProvider e) {
        var lowCode = e.getDiagnosticCode();
        if (lowCode == SqlServiceCode.ERR_CONFLICT_ON_WRITE_PRESERVE) {
            throw new IllegalStateException("illegal diagnosticCode. " + e.toString());
        }
        return TgTmRetryInstruction.ofNotRetryable("LTX diagnosticCode=" + lowCode);
    }

    protected TgTmRetryInstruction testRtx(TsurugiTransaction transaction, TsurugiDiagnosticCodeProvider e) {
        var lowCode = e.getDiagnosticCode();
        if (lowCode == SqlServiceCode.ERR_CONFLICT_ON_WRITE_PRESERVE) {
            throw new IllegalStateException("illegal diagnosticCode. " + e.toString());
        }
        return TgTmRetryInstruction.ofNotRetryable("RTX diagnosticCode=" + lowCode);
    }

    protected TgTmRetryInstruction testOther(TsurugiTransaction transaction, TsurugiDiagnosticCodeProvider e) {
        var lowCode = e.getDiagnosticCode();
        return TgTmRetryInstruction.ofNotRetryable("OTHER diagnosticCode=" + lowCode);
    }
}
