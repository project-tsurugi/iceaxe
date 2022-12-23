package com.tsurugidb.iceaxe.transaction.manager.option;

import java.util.Objects;
import java.util.function.BiPredicate;

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
public class TsurugiDefaultRetryPredicate implements BiPredicate<TsurugiTransaction, TsurugiDiagnosticCodeProvider> {

    private static BiPredicate<TsurugiTransaction, TsurugiDiagnosticCodeProvider> instance = new TsurugiDefaultRetryPredicate();

    /**
     * get default retry predicate
     *
     * @return retry predicate
     */
    public static BiPredicate<TsurugiTransaction, TsurugiDiagnosticCodeProvider> getInstance() {
        return instance;
    }

    /**
     * set default retry predicate
     *
     * @param defaultRetryPredicate retry predicate
     */
    public static void setInstance(@Nonnull BiPredicate<TsurugiTransaction, TsurugiDiagnosticCodeProvider> defaultRetryPredicate) {
        instance = Objects.requireNonNull(defaultRetryPredicate);
    }

    @Override
    public boolean test(TsurugiTransaction transaction, TsurugiDiagnosticCodeProvider e) {
        Boolean r = testCommon(transaction, e);
        if (r != null) {
            return r.booleanValue();
        }

        var option = transaction.getTransactionOption();
        var type = option.type();
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

    @Nullable
    protected Boolean testCommon(TsurugiTransaction transaction, TsurugiDiagnosticCodeProvider e) {
        var lowCode = e.getDiagnosticCode();
        if (lowCode == SqlServiceCode.ERR_ABORTED_RETRYABLE) {
            return true;
        }
        return null;
    }

    protected boolean testOcc(TsurugiTransaction transaction, TsurugiDiagnosticCodeProvider e) {
        var lowCode = e.getDiagnosticCode();
        if (lowCode == SqlServiceCode.ERR_CONFLICT_ON_WRITE_PRESERVE) {
            return true;
        }
        return false;
    }

    protected boolean testLtx(TsurugiTransaction transaction, TsurugiDiagnosticCodeProvider e) {
        return false;
    }

    protected boolean testRtx(TsurugiTransaction transaction, TsurugiDiagnosticCodeProvider e) {
        return false;
    }

    protected boolean testOther(TsurugiTransaction transaction, TsurugiDiagnosticCodeProvider e) {
        return false;
    }
}
