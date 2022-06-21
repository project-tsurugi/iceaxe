package com.tsurugidb.iceaxe.transaction;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * TgTransactionOption list
 */
@ThreadSafe
public class TgTxOptionList implements TgTxOptionSupplier {

    /**
     * create TgTransactionOptionList
     * 
     * @param transactionOptionList options
     * @return TgTransactionOptionList
     */
    public static TgTxOptionList of(TgTxOption... transactionOptionList) {
        if (transactionOptionList == null || transactionOptionList.length == 0) {
            throw new IllegalArgumentException("transactionOptionList is null or empty");
        }
        return new TgTxOptionList(List.of(transactionOptionList));
    }

    /**
     * create TgTransactionOptionList
     * 
     * @param transactionOptionList options
     * @return TgTransactionOptionList
     */
    public static TgTxOptionList of(List<TgTxOption> transactionOptionList) {
        if (transactionOptionList == null || transactionOptionList.isEmpty()) {
            throw new IllegalArgumentException("transactionOptionList is null or empty");
        }
        return new TgTxOptionList(transactionOptionList);
    }

    private final List<TgTxOption> transactionOptionList;

    /**
     * TgTransactionOption list
     * 
     * @param transactionOptionList options
     */
    public TgTxOptionList(List<TgTxOption> transactionOptionList) {
        this.transactionOptionList = transactionOptionList;
    }

    @Override
    public TgTxOption get(int attempt, TsurugiTransactionException e) {
        if (attempt == 0) {
            return transactionOptionList.get(0);
        }

        if (attempt < transactionOptionList.size() && e.isRetryable()) {
            return transactionOptionList.get(attempt);
        }

        return null;
    }
}
