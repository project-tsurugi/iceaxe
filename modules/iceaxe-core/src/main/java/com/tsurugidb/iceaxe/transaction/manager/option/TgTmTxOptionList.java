package com.tsurugidb.iceaxe.transaction.manager.option;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.retry.TgTmRetryInstruction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * {@link TgTxOption} list.
 */
@ThreadSafe
public class TgTmTxOptionList extends TgTmTxOptionSupplier {

    /**
     * create TgTmTxOptionList.
     *
     * @param txOptions transaction options
     * @return TgTmTxOptionList
     */
    public static TgTmTxOptionList of(TgTxOption... txOptions) {
        if (txOptions == null || txOptions.length == 0) {
            throw new IllegalArgumentException("txOptions is null or empty");
        }
        return new TgTmTxOptionList(List.of(txOptions));
    }

    /**
     * create TgTmTxOptionList.
     *
     * @param txOptions transaction options
     * @return TgTmTxOptionList
     */
    public static TgTmTxOptionList of(List<TgTxOption> txOptions) {
        if (txOptions == null || txOptions.isEmpty()) {
            throw new IllegalArgumentException("txOptions is null or empty");
        }
        return new TgTmTxOptionList(txOptions);
    }

    private final List<TgTxOption> txOptionList;

    /**
     * Creates a new instance.
     *
     * @param txOptionList transaction options
     */
    public TgTmTxOptionList(List<TgTxOption> txOptionList) {
        this.txOptionList = txOptionList;
    }

    @Override
    protected TgTmTxOption computeFirstTmOption(Object executeInfo) {
        return TgTmTxOption.execute(txOptionList.get(0), null);
    }

    @Override
    protected TgTmTxOption computeRetryTmOption(Object executeInfo, int attempt, TsurugiTransactionException e, TgTmRetryInstruction retryInstruction) {
        if (attempt < txOptionList.size()) {
            return TgTmTxOption.execute(txOptionList.get(attempt), retryInstruction);
        }
        return TgTmTxOption.retryOver(retryInstruction);
    }

    @Override
    protected String getDefaultDescription() {
        return txOptionList.toString();
    }
}
