package com.tsurugidb.iceaxe.transaction.manager.option;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * {@link TgTxOption} list
 */
@ThreadSafe
public class TgTmTxOptionList extends TgTmTxOptionSupplier {

    /**
     * create TgTmTxOptionList
     *
     * @param txOptionList options
     * @return TgTmTxOptionList
     */
    public static TgTmTxOptionList of(TgTxOption... txOptionList) {
        if (txOptionList == null || txOptionList.length == 0) {
            throw new IllegalArgumentException("txOptionList is null or empty");
        }
        return new TgTmTxOptionList(List.of(txOptionList));
    }

    /**
     * create TgTmTxOptionList
     *
     * @param txOptionList options
     * @return TgTmTxOptionList
     */
    public static TgTmTxOptionList of(List<TgTxOption> txOptionList) {
        if (txOptionList == null || txOptionList.isEmpty()) {
            throw new IllegalArgumentException("txOptionList is null or empty");
        }
        return new TgTmTxOptionList(txOptionList);
    }

    private final List<TgTxOption> txOptionList;

    /**
     * TgTxOption list
     *
     * @param txOptionList options
     */
    public TgTmTxOptionList(List<TgTxOption> txOptionList) {
        this.txOptionList = txOptionList;
    }

    @Override
    protected TgTmTxOption computeFirstTmOption() {
        return TgTmTxOption.execute(txOptionList.get(0));
    }

    @Override
    protected TgTmTxOption computeRetryTmOption(int attempt, TsurugiTransactionException e) {
        if (attempt < txOptionList.size()) {
            return TgTmTxOption.execute(txOptionList.get(attempt));
        }
        return TgTmTxOption.retryOver();
    }
}
