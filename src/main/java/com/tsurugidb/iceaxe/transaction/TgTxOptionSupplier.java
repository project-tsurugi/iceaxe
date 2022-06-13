package com.tsurugidb.iceaxe.transaction;

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
     * @return supplier
     */
    public static TgTxOptionSupplier ofAlways(TgTxOption transactionOption) {
        return (var attempt, var e) -> {
            if (attempt == 0 || e.isRetryable()) {
                return transactionOption;
            }
            return null;
        };
    }

    /**
     * get Transaction Option
     * 
     * @param attempt attempt count
     * @param e       transaction exception (null if attempt==0)
     * @return Transaction Option. TODO 翻訳+++: トランザクションを実行しない場合はnull
     */
    public TgTxOption get(int attempt, TsurugiTransactionException e);
}
