package com.tsurugidb.iceaxe.transaction.manager.option;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * TgTxOption list
 */
@ThreadSafe
public class TgTxOptionList extends TgTxOptionSupplier {

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
    protected TgTxState computeFirstTransactionState() {
        return TgTxState.execute(transactionOptionList.get(0));
    }

    @Override
    protected TgTxState computeRetryTransactionState(int attempt, TsurugiTransactionException e) {
        if (attempt < transactionOptionList.size()) {
            return TgTxState.execute(transactionOptionList.get(attempt));
        }
        return TgTxState.retryOver();
    }
}
