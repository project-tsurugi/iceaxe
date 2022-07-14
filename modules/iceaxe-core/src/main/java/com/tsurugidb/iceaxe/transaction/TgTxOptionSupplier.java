package com.tsurugidb.iceaxe.transaction;

import javax.annotation.Nonnull;

/**
 * {@link TgTxOption} supplier
 */
@FunctionalInterface
public interface TgTxOptionSupplier {

    /**
     * create TsurugiTransactionOptionSupplier
     * 
     * @param transactionOptionList options
     * @return supplier
     */
    public static TgTxOptionSupplier of(TgTxOption... transactionOptionList) {
        return TgTxOptionList.of(transactionOptionList);
    }

    /**
     * create TsurugiTransactionOptionSupplier
     * 
     * @param transactionOption option
     * @param attemtMaxCount    attempt max count
     * @return supplier
     */
    public static TgTxOptionSupplier ofAlways(TgTxOption transactionOption, int attemtMaxCount) {
        return (attempt, e) -> {
            if (attempt == 0) {
                return TgTxState.execute(transactionOption);
            }

            if (e.isRetryable()) {
                if (attempt < attemtMaxCount) {
                    return TgTxState.execute(transactionOption);
                }
                return TgTxState.retryOver();
            }

            return TgTxState.notRetryable();
        };
    }

    /**
     * get Transaction Option
     * 
     * @param attempt attempt number
     * @param e       transaction exception (null if attempt==0)
     * @return Transaction Option
     */
    @Nonnull
    public TgTxState get(int attempt, TsurugiTransactionException e);
}
