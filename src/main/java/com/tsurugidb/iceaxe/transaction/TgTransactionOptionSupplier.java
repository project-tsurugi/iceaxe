package com.tsurugidb.iceaxe.transaction;

/**
 * {@link TgTransactionOption} supplier
 */
@FunctionalInterface
public interface TgTransactionOptionSupplier {

    /**
     * create TsurugiTransactionOptionSupplier
     * 
     * @param transactionOptionList options
     * @return supplier
     */
    public static TgTransactionOptionSupplier of(TgTransactionOption... transactionOptionList) {
        return TgTransactionOptionList.of(transactionOptionList);
    }

    /**
     * create TsurugiTransactionOptionSupplier
     * 
     * @param transactionOption option
     * @return supplier
     */
    public static TgTransactionOptionSupplier ofAlways(TgTransactionOption transactionOption) {
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
    public TgTransactionOption get(int attempt, TsurugiTransactionException e);
}
