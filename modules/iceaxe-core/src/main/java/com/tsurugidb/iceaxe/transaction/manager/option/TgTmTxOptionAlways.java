package com.tsurugidb.iceaxe.transaction.manager.option;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.retry.TgTmRetryInstruction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * Always the same {@link TgTxOption}
 */
@ThreadSafe
public class TgTmTxOptionAlways extends TgTmTxOptionSupplier {

    /**
     * create TgTmTxOptionAlways
     *
     * @param txOption       transaction option
     * @param attemptMaxCount attempt max count
     * @return TgTmTxOptionAlways
     */
    public static TgTmTxOptionAlways of(TgTxOption txOption, int attemptMaxCount) {
        if (txOption == null) {
            throw new IllegalArgumentException("txOption is null");
        }
        return new TgTmTxOptionAlways(txOption, attemptMaxCount);
    }

    protected final TgTxOption txOption;
    protected final int attemptMaxCount;

    /**
     * TgTxOption list
     *
     * @param txOption       transaction option
     * @param attemptMaxCount attempt max count
     */
    public TgTmTxOptionAlways(TgTxOption txOption, int attemptMaxCount) {
        this.txOption = txOption;
        this.attemptMaxCount = attemptMaxCount;
    }

    @Override
    protected TgTmTxOption computeFirstTmOption(Object executeInfo) {
        return TgTmTxOption.execute(txOption, null);
    }

    @Override
    protected TgTmTxOption computeRetryTmOption(Object executeInfo, int attempt, TsurugiTransactionException e, TgTmRetryInstruction retryInstruction) {
        if (attempt < attemptMaxCount) {
            return TgTmTxOption.execute(txOption, retryInstruction);
        }
        return TgTmTxOption.retryOver(retryInstruction);
    }

    @Override
    protected String getDefaultDescription() {
        return "always(" + txOption + ", " + attemptMaxCount + ")";
    }
}
