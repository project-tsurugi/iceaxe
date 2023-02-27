package com.tsurugidb.iceaxe.transaction.manager.option;

import javax.annotation.Nonnull;

import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * Tsurugi transaction option for {@link TsurugiTransactionManager}
 */
public final class TgTmTxOption {

    private static final TgTmTxOption RETRY_OVER = new TgTmTxOption(true, null);
    private static final TgTmTxOption NOT_RETRYABLE = new TgTmTxOption(false, null);

    /**
     * Creates a new instance.
     *
     * @param txOption transaction option
     * @return tm option
     */
    public static TgTmTxOption execute(@Nonnull TgTxOption txOption) {
        return new TgTmTxOption(false, txOption);
    }

    /**
     * returns tm option for retry over
     *
     * @return tm option
     */
    public static TgTmTxOption retryOver() {
        return RETRY_OVER;
    }

    /**
     * returns tm option for not retryable
     *
     * @return tm option
     */
    public static TgTmTxOption notRetryable() {
        return NOT_RETRYABLE;
    }

    private final boolean isRetryOver;
    private final TgTxOption txOption;

    private TgTmTxOption(boolean isRetryOver, TgTxOption txOption) {
        this.isRetryOver = isRetryOver;
        this.txOption = txOption;
    }

    /**
     * get executable
     *
     * @return true: executable
     */
    public boolean isExecute() {
        return this.txOption != null;
    }

    /**
     * get retry over
     *
     * @return true: retry over
     */
    public boolean isRetryOver() {
        return this.isRetryOver;
    }

    /**
     * get transaction option
     *
     * @return transaction option
     */
    public TgTxOption getOption() {
        return this.txOption;
    }

    @Override
    public String toString() {
        if (this.isRetryOver) {
            return "RETRY_OVER";
        }
        if (this.txOption == null) {
            return "NOT_RETRYABLE";
        }
        return txOption.toString();
    }
}
