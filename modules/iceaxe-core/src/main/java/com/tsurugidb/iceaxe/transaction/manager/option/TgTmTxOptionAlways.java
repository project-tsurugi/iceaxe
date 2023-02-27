package com.tsurugidb.iceaxe.transaction.manager.option;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * Always the same {@link TgTxOption}
 */
@ThreadSafe
public class TgTmTxOptionAlways extends TgTmTxOptionSupplier {

    /**
     * create TgTmTxOptionAlways
     *
     * @param txOption       transaction option
     * @param attemtMaxCount attempt max count
     * @return TgTmTxOptionAlways
     */
    public static TgTmTxOptionAlways of(TgTxOption txOption, int attemtMaxCount) {
        if (txOption == null) {
            throw new IllegalArgumentException("txOption is null");
        }
        return new TgTmTxOptionAlways(txOption, attemtMaxCount);
    }

    protected final TgTxOption txOption;
    protected final int attemtMaxCount;

    /**
     * TgTxOption list
     *
     * @param txOption       transaction option
     * @param attemtMaxCount attempt max count
     */
    public TgTmTxOptionAlways(TgTxOption txOption, int attemtMaxCount) {
        this.txOption = txOption;
        this.attemtMaxCount = attemtMaxCount;
    }

    @Override
    protected TgTmTxOption computeFirstTmOption() {
        return TgTmTxOption.execute(txOption);
    }

    @Override
    protected TgTmTxOption computeRetryTmOption(int attempt, TsurugiTransactionException e) {
        if (attempt < attemtMaxCount) {
            return TgTmTxOption.execute(txOption);
        }
        return TgTmTxOption.retryOver();
    }
}
