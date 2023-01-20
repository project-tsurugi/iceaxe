package com.tsurugidb.iceaxe.transaction.manager.option;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * Always the same TgTxOption
 */
@ThreadSafe
public class TgTmTxOptionAlways extends TgTmTxOptionSupplier {

    /**
     * create TgTmTxOptionAlways
     *
     * @param transactionOption transaction option
     * @param attemtMaxCount    attempt max count
     * @return TgTmTxOptionAlways
     */
    public static TgTmTxOptionAlways of(TgTxOption transactionOption, int attemtMaxCount) {
        if (transactionOption == null) {
            throw new IllegalArgumentException("transactionOption is null");
        }
        return new TgTmTxOptionAlways(transactionOption, attemtMaxCount);
    }

    protected final TgTxOption transactionOption;
    protected final int attemtMaxCount;

    /**
     * TgTransactionOption list
     *
     * @param transactionOption transaction option
     * @param attemtMaxCount    attempt max count
     */
    public TgTmTxOptionAlways(TgTxOption transactionOption, int attemtMaxCount) {
        this.transactionOption = transactionOption;
        this.attemtMaxCount = attemtMaxCount;
    }

    @Override
    protected TgTmTxOption computeFirstTmOption() {
        return TgTmTxOption.execute(transactionOption);
    }

    @Override
    protected TgTmTxOption computeRetryTmOption(int attempt, TsurugiTransactionException e) {
        if (attempt < attemtMaxCount) {
            return TgTmTxOption.execute(transactionOption);
        }
        return TgTmTxOption.retryOver();
    }
}
