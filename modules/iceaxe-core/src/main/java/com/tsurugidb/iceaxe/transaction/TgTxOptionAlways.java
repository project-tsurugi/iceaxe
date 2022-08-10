package com.tsurugidb.iceaxe.transaction;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * Always the same TgTxOption
 */
@ThreadSafe
public class TgTxOptionAlways implements TgTxOptionSupplier {

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

    private final TgTxOption transactionOption;
    private final int attemtMaxCount;

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
    public TgTxState get(int attempt, TsurugiTransactionException e) {
        if (attempt == 0) {
            return TgTxState.execute(transactionOption);
        }

        if (isRetryable(e)) {
            if (attempt < attemtMaxCount) {
                return TgTxState.execute(transactionOption);
            }
            return TgTxState.retryOver();
        }

        return TgTxState.notRetryable();
    }
}
