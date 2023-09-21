package com.tsurugidb.iceaxe.transaction.manager.option;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.manager.retry.TgTmRetryInstruction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * Tsurugi transaction option for {@link TsurugiTransactionManager}.
 */
public final class TgTmTxOption {

    /**
     * Creates a new instance.
     *
     * @param txOption         transaction option, {@code null} if first transaction
     * @param retryInstruction retry instruction
     * @return tm option
     */
    public static TgTmTxOption execute(@Nonnull TgTxOption txOption, @Nullable TgTmRetryInstruction retryInstruction) {
        return new TgTmTxOption(false, Objects.requireNonNull(txOption), retryInstruction);
    }

    /**
     * returns tm option for retry over.
     *
     * @param retryInstruction retry instruction
     * @return tm option
     */
    public static TgTmTxOption retryOver(@Nonnull TgTmRetryInstruction retryInstruction) {
        return new TgTmTxOption(true, null, Objects.requireNonNull(retryInstruction));
    }

    /**
     * returns tm option for not retryable.
     *
     * @param retryInstruction retry instruction
     * @return tm option
     */
    public static TgTmTxOption notRetryable(@Nonnull TgTmRetryInstruction retryInstruction) {
        return new TgTmTxOption(false, null, Objects.requireNonNull(retryInstruction));
    }

    private final boolean isRetryOver;
    private final TgTxOption txOption;
    private final TgTmRetryInstruction retryInstruction;

    private TgTmTxOption(boolean isRetryOver, TgTxOption txOption, TgTmRetryInstruction retryInstruction) {
        this.isRetryOver = isRetryOver;
        this.txOption = txOption;
        this.retryInstruction = retryInstruction;
    }

    /**
     * get executable.
     *
     * @return true: executable
     */
    public boolean isExecute() {
        return this.txOption != null;
    }

    /**
     * get retry over.
     *
     * @return true: retry over
     */
    public boolean isRetryOver() {
        return this.isRetryOver;
    }

    /**
     * get transaction option.
     *
     * @return transaction option
     */
    public @Nullable TgTxOption getTransactionOption() {
        return this.txOption;
    }

    /**
     * get retry instruction.
     *
     * @return retry instruction
     */
    public @Nullable TgTmRetryInstruction getRetryInstruction() {
        return this.retryInstruction;
    }

    @Override
    public String toString() {
        if (this.isRetryOver || this.txOption == null) {
            if (this.retryInstruction != null) {
                return retryInstruction.toString();
            }
            if (this.isRetryOver) {
                return "RETRY_OVER";
            } else {
                return "NOT_RETRYABLE";
            }
        }

        if (this.retryInstruction != null) {
            return txOption + "(" + retryInstruction + ")";
        } else {
            return txOption.toString();
        }
    }
}
