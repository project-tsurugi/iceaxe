package com.tsurugidb.iceaxe.transaction.manager.option;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * Always the same TgTxOption
 */
@ThreadSafe
public class TgTxOptionAlways extends TgTxOptionSupplier {

    /**
     * create TgTxOptionAlways
     *
     * @param transactionOption transaction option
     * @param attemtMaxCount    attempt max count
     * @return TgTxOptionAlways
     */
    public static TgTxOptionAlways of(TgTxOption transactionOption, int attemtMaxCount) {
        if (transactionOption == null) {
            throw new IllegalArgumentException("transactionOption is null");
        }
        return new TgTxOptionAlways(transactionOption, attemtMaxCount);
    }

    protected final TgTxOption transactionOption;
    protected final int attemtMaxCount;

    /**
     * TgTransactionOption list
     *
     * @param transactionOption transaction option
     * @param attemtMaxCount    attempt max count
     */
    public TgTxOptionAlways(TgTxOption transactionOption, int attemtMaxCount) {
        this.transactionOption = transactionOption;
        this.attemtMaxCount = attemtMaxCount;
    }

    @Override
    protected TgTxState computeFirstTransactionState() {
        return TgTxState.execute(transactionOption);
    }

    @Override
    protected TgTxState computeRetryTransactionState(int attempt, TsurugiTransactionException e) {
        if (attempt < attemtMaxCount) {
            return TgTxState.execute(transactionOption);
        }
        return TgTxState.retryOver();
    }
}
