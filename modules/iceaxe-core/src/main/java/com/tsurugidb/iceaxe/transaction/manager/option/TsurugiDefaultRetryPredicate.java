package com.tsurugidb.iceaxe.transaction.manager.option;

import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;
import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * default retry predicate
 */
@ThreadSafe
public class TsurugiDefaultRetryPredicate implements Predicate<TsurugiDiagnosticCodeProvider> {

    private static Predicate<TsurugiDiagnosticCodeProvider> instance = new TsurugiDefaultRetryPredicate();

    /**
     * get default retry predicate
     *
     * @return retry predicate
     */
    public static Predicate<TsurugiDiagnosticCodeProvider> getInstance() {
        return instance;
    }

    /**
     * set default retry predicate
     *
     * @param defaultRetryPredicate retry predicate
     */
    public static void setInstance(@Nonnull Predicate<TsurugiDiagnosticCodeProvider> defaultRetryPredicate) {
        instance = Objects.requireNonNull(defaultRetryPredicate);
    }

    private static final Set<DiagnosticCode> RETRYABLE_SET = Set.of(//
            SqlServiceCode.ERR_ABORTED_RETRYABLE, //
            SqlServiceCode.ERR_CONFLICT_ON_WRITE_PRESERVE //
    );

    @Override
    public boolean test(TsurugiDiagnosticCodeProvider e) {
        var lowCode = e.getDiagnosticCode();
        return RETRYABLE_SET.contains(lowCode);
    }
}
