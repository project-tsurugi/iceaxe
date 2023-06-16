package com.tsurugidb.iceaxe.example;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.retry.TsurugiDefaultRetryPredicate;
import com.tsurugidb.iceaxe.transaction.manager.retry.TsurugiTmRetryPredicate;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * {@link TsurugiTmRetryPredicate} example.
 *
 * @see Example04TmSetting
 */
public class Example04TmRetryPredicate {

    void predicate(TgTmSetting setting) {
        var supplier = setting.getTransactionOptionSupplier();
        supplier.setRetryPredicate(new TsurugiDefaultRetryPredicate() {
            @Override
            protected boolean isRetryable(TsurugiTransactionException e) {
                var code = e.getDiagnosticCode();
                if (code == SqlServiceCode.ERR_ABORTED) {
                    return true;
                }
                return super.isRetryable(e);
            }
        });
    }
}
