package com.tsurugidb.iceaxe.transaction.manager.option;

import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * {@link TgTxOption} supplier
 */
public abstract class TgTxOptionSupplier {

    /**
     * create TsurugiTransactionOptionSupplier
     *
     * @param transactionOptionList options
     * @return supplier
     */
    public static TgTxOptionSupplier of(TgTxOption... transactionOptionList) {
        return TgTxOptionList.of(transactionOptionList);
    }

    /**
     * create TsurugiTransactionOptionSupplier
     *
     * @param transactionOption option
     * @param attemtMaxCount    attempt max count
     * @return supplier
     */
    public static TgTxOptionSupplier ofAlways(TgTxOption transactionOption, int attemtMaxCount) {
        return TgTxOptionAlways.of(transactionOption, attemtMaxCount);
    }

    /**
     * {@link TgTxState} listener
     */
    @FunctionalInterface
    public interface TgTxStateListener {
        /**
         * accept.
         *
         * @param attempt attempt number
         * @param e       transaction exception (null if attempt==0)
         * @param state   transaction state
         */
        public void accept(int attempt, TsurugiTransactionException e, TgTxState state);
    }

    private Predicate<TsurugiDiagnosticCodeProvider> retryPredicate;
    private TgTxStateListener stateListener;

    /**
     * Creates a new instance.
     */
    public TgTxOptionSupplier() {
        this(TsurugiDefaultRetryPredicate.getInstance());
    }

    /**
     * Creates a new instance.
     *
     * @param predicate retry predicate
     */
    public TgTxOptionSupplier(Predicate<TsurugiDiagnosticCodeProvider> predicate) {
        setRetryPredicate(predicate);
    }

    /**
     * set retry predicate
     *
     * @param predicate retry predicate
     */
    public void setRetryPredicate(@Nonnull Predicate<TsurugiDiagnosticCodeProvider> predicate) {
        if (predicate == null) {
            throw new IllegalArgumentException("predicate is null");
        }
        this.retryPredicate = predicate;
    }

    /**
     * set state listener
     *
     * @param listener state listener
     */
    public void setStateListener(@Nullable TgTxStateListener listener) {
        this.stateListener = listener;
    }

    /**
     * get Transaction Option
     *
     * @param attempt attempt number
     * @param e       transaction exception (null if attempt==0)
     * @return Transaction Option
     */
    @Nonnull
    public final TgTxState get(int attempt, TsurugiTransactionException e) {
        var state = computeTransactionState(attempt, e);
        if (this.stateListener != null) {
            stateListener.accept(attempt, e, state);
        }
        return state;
    }

    protected TgTxState computeTransactionState(int attempt, TsurugiTransactionException e) {
        if (attempt == 0) {
            return computeFirstTransactionState();
        }

        if (isRetryable(e)) {
            return computeRetryTransactionState(attempt, e);
        }

        return TgTxState.notRetryable();
    }

    protected abstract TgTxState computeFirstTransactionState();

    protected abstract TgTxState computeRetryTransactionState(int attempt, TsurugiTransactionException e);

    /**
     * whether to retry
     *
     * @param e Transaction Exception
     * @return true: retryable
     */
    protected boolean isRetryable(TsurugiTransactionException e) {
        return retryPredicate.test(e);
    }
}
