package com.tsurugidb.iceaxe.transaction.manager.option;

import java.util.Set;
import java.util.function.Predicate;

import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;
import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * default retry predicate
 */
public class TsurugiDefaultRetryPredicate implements Predicate<TsurugiDiagnosticCodeProvider> {

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
