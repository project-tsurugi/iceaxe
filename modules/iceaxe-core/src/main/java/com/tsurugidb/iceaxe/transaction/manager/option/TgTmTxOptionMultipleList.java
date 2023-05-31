package com.tsurugidb.iceaxe.transaction.manager.option;

import java.util.TreeMap;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.retry.TgTmRetryInstruction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * {@link TgTxOption} multiple list
 */
@ThreadSafe
public class TgTmTxOptionMultipleList extends TgTmTxOptionSupplier {

    /**
     * create TgTmTxOptionMultipleList
     *
     * @return TgTmTxOptionMultipleList
     */
    public static TgTmTxOptionMultipleList of() {
        return new TgTmTxOptionMultipleList();
    }

    private final TreeMap<Integer, TgTxOption> txOptionMap = new TreeMap<>();
    private int totalSize = 0;

    /**
     * add transaction option
     *
     * @param txOption transaction option
     * @param size     size
     * @return this
     */
    public TgTmTxOptionMultipleList add(TgTxOption txOption, int size) {
        if (txOption == null) {
            throw new IllegalArgumentException("txOption == null");
        }
        if (size < 1) {
            throw new IllegalArgumentException("size < 1 (size=" + size + ")");
        }

        try {
            this.totalSize = Math.addExact(this.totalSize, size);
        } catch (ArithmeticException e) {
            if (this.totalSize != Integer.MAX_VALUE) {
                this.totalSize = Integer.MAX_VALUE;
            } else {
                throw new IllegalArgumentException("size over", e);
            }
        }
        txOptionMap.put(this.totalSize - 1, txOption);

        return this;
    }

    /**
     * get transaction option
     *
     * @param attempt attempt number
     * @return transaction option
     */
    public @Nullable TgTxOption findTxOption(int attempt) {
        var entry = txOptionMap.ceilingEntry(attempt);
        return (entry != null) ? entry.getValue() : null;
    }

    @Override
    protected TgTmTxOption computeFirstTmOption(Object executeInfo) {
        var txOption = findTxOption(0);
        if (txOption == null) {
            throw new IllegalStateException("unspecified txOption");
        }
        return TgTmTxOption.execute(txOption, null);
    }

    @Override
    protected TgTmTxOption computeRetryTmOption(Object executeInfo, int attempt, TsurugiTransactionException e, TgTmRetryInstruction retryInstruction) {
        var txOption = findTxOption(attempt);
        if (txOption == null) {
            return TgTmTxOption.retryOver(retryInstruction);
        }
        return TgTmTxOption.execute(txOption, retryInstruction);
    }

    @Override
    protected String getDefaultDescription() {
        var sb = new StringBuilder(128);

        int prev = -1;
        for (var entry : txOptionMap.entrySet()) {
            int n = entry.getKey();
            var txOption = entry.getValue();
            int count = n - prev;

            if (sb.length() != 0) {
                sb.append(", ");
            }
            sb.append(txOption);
            sb.append("*");
            sb.append(count);

            prev = n;
        }

        return sb.toString();
    }
}
