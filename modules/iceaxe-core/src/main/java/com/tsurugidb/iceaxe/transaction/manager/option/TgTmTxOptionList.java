package com.tsurugidb.iceaxe.transaction.manager.option;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * TgTxOption list
 */
@ThreadSafe
public class TgTmTxOptionList extends TgTmTxOptionSupplier {

    /**
     * create TgTmTxOptionList
     *
     * @param transactionOptionList options
     * @return TgTmTxOptionList
     */
    public static TgTmTxOptionList of(TgTxOption... transactionOptionList) {
        if (transactionOptionList == null || transactionOptionList.length == 0) {
            throw new IllegalArgumentException("transactionOptionList is null or empty");
        }
        return new TgTmTxOptionList(List.of(transactionOptionList));
    }

    /**
     * create TgTmTxOptionList
     *
     * @param transactionOptionList options
     * @return TgTmTxOptionList
     */
    public static TgTmTxOptionList of(List<TgTxOption> transactionOptionList) {
        if (transactionOptionList == null || transactionOptionList.isEmpty()) {
            throw new IllegalArgumentException("transactionOptionList is null or empty");
        }
        return new TgTmTxOptionList(transactionOptionList);
    }

    private final List<TgTxOption> transactionOptionList;

    /**
     * TgTransactionOption list
     *
     * @param transactionOptionList options
     */
    public TgTmTxOptionList(List<TgTxOption> transactionOptionList) {
        this.transactionOptionList = transactionOptionList;
    }

    @Override
    protected TgTmTxOption computeFirstTmOption() {
        return TgTmTxOption.execute(transactionOptionList.get(0));
    }

    @Override
    protected TgTmTxOption computeRetryTmOption(int attempt, TsurugiTransactionException e) {
        if (attempt < transactionOptionList.size()) {
            return TgTmTxOption.execute(transactionOptionList.get(attempt));
        }
        return TgTmTxOption.retryOver();
    }
}
