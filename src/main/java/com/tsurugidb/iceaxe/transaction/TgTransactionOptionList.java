package com.tsurugidb.iceaxe.transaction;

import java.util.List;

/**
 * TgTransactionOption list
 */
public class TgTransactionOptionList implements TgTransactionOptionSupplier {

    /**
     * create TgTransactionOptionList
     * 
     * @param transactionOptionList options
     * @return TgTransactionOptionList
     */
    public static TgTransactionOptionList of(TgTransactionOption... transactionOptionList) {
        if (transactionOptionList == null || transactionOptionList.length == 0) {
            throw new IllegalArgumentException("transactionOptionList is null or empty");
        }
        return new TgTransactionOptionList(List.of(transactionOptionList));
    }

    /**
     * create TgTransactionOptionList
     * 
     * @param transactionOptionList options
     * @return TgTransactionOptionList
     */
    public static TgTransactionOptionList of(List<TgTransactionOption> transactionOptionList) {
        if (transactionOptionList == null || transactionOptionList.isEmpty()) {
            throw new IllegalArgumentException("transactionOptionList is null or empty");
        }
        return new TgTransactionOptionList(transactionOptionList);
    }

    private final List<TgTransactionOption> transactionOptionList;

    /**
     * TgTransactionOption list
     * 
     * @param transactionOptionList options
     */
    public TgTransactionOptionList(List<TgTransactionOption> transactionOptionList) {
        this.transactionOptionList = transactionOptionList;
    }

    @Override
    public TgTransactionOption get(int attempt, TsurugiTransactionException e) {
        if (attempt == 0) {
            return transactionOptionList.get(0);
        }

        if (attempt < transactionOptionList.size() && e.isRetryable()) {
            return transactionOptionList.get(attempt);
        }

        return null;
    }
}
