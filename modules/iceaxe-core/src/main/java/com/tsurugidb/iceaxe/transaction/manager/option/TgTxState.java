package com.tsurugidb.iceaxe.transaction.manager.option;

import javax.annotation.Nonnull;

import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * Tsurugi Transaction State
 */
public final class TgTxState {

    private static final TgTxState RETRY_OVER = new TgTxState(true, null);
    private static final TgTxState NOT_RETRYABLE = new TgTxState(false, null);

    /**
     * create Transaction State
     *
     * @param option transaction option
     * @return state
     */
    public static TgTxState execute(@Nonnull TgTxOption option) {
        return new TgTxState(false, option);
    }

    /**
     * returns Transaction State for retry over
     *
     * @return state
     */
    public static TgTxState retryOver() {
        return RETRY_OVER;
    }

    /**
     * returns Transaction State for not retryable
     *
     * @return state
     */
    public static TgTxState notRetryable() {
        return NOT_RETRYABLE;
    }

    private final boolean isRetryOver;
    private final TgTxOption option;

    private TgTxState(boolean isRetryOver, TgTxOption option) {
        this.isRetryOver = isRetryOver;
        this.option = option;
    }

    /**
     * get executable
     *
     * @return true: executable
     */
    public boolean isExecute() {
        return this.option != null;
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
        return this.option;
    }

    @Override
    public String toString() {
        if (this.isRetryOver) {
            return "RETRY_OVER";
        }
        if (this.option == null) {
            return "NOT_RETRYABLE";
        }
        return option.toString();
    }
}
