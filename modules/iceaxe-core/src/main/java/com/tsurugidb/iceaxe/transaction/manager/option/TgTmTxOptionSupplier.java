package com.tsurugidb.iceaxe.transaction.manager.option;

import java.util.function.BiPredicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * {@link TgTxOption} supplier
 */
public abstract class TgTmTxOptionSupplier {

    /**
     * create TgTmTxOptionSupplier
     *
     * @param txOptionList transaction options
     * @return supplier
     */
    public static TgTmTxOptionSupplier of(TgTxOption... txOptionList) {
        return TgTmTxOptionList.of(txOptionList);
    }

    /**
     * create TgTmTxOptionSupplier
     *
     * @param txOption transaction option
     * @return supplier
     */
    public static TgTmTxOptionSupplier ofAlways(TgTxOption txOption) {
        return TgTmTxOptionAlways.of(txOption, Integer.MAX_VALUE);
    }

    /**
     * create TgTmTxOptionSupplier
     *
     * @param txOption       transaction option
     * @param attemtMaxCount attempt max count
     * @return supplier
     */
    public static TgTmTxOptionSupplier ofAlways(TgTxOption txOption, int attemtMaxCount) {
        return TgTmTxOptionAlways.of(txOption, attemtMaxCount);
    }

    /**
     * create TgTmTxOptionSupplier
     *
     * @param txOption transaction option
     * @param size     size
     * @return supplier
     */
    public static TgTmTxOptionSupplier of(TgTxOption txOption, int size) {
        return TgTmTxOptionMultipleList.of().add(txOption, size);
    }

    /**
     * create TgTmTxOptionSupplier
     *
     * @param txOption1 transaction option
     * @param size1     size
     * @param txOption2 transaction option
     * @param size2     size
     * @return supplier
     */
    public static TgTmTxOptionSupplier of(TgTxOption txOption1, int size1, TgTxOption txOption2, int size2) {
        return TgTmTxOptionMultipleList.of().add(txOption1, size1).add(txOption2, size2);
    }

    /**
     * {@link TgTmTxOption} listener
     */
    @FunctionalInterface
    public interface TgTmOptionListener {
        /**
         * accept.
         *
         * @param attempt  attempt number
         * @param e        transaction exception (null if attempt==0)
         * @param tmOption tm option
         */
        public void accept(int attempt, TsurugiTransactionException e, TgTmTxOption tmOption);
    }

    private BiPredicate<TsurugiTransaction, TsurugiDiagnosticCodeProvider> retryPredicate;
    private TgTmOptionListener tmOptionListener;

    /**
     * Creates a new instance.
     */
    public TgTmTxOptionSupplier() {
        this(TsurugiDefaultRetryPredicate.getInstance());
    }

    /**
     * Creates a new instance.
     *
     * @param predicate retry predicate
     */
    public TgTmTxOptionSupplier(BiPredicate<TsurugiTransaction, TsurugiDiagnosticCodeProvider> predicate) {
        setRetryPredicate(predicate);
    }

    /**
     * set retry predicate
     *
     * @param predicate retry predicate
     */
    public void setRetryPredicate(@Nonnull BiPredicate<TsurugiTransaction, TsurugiDiagnosticCodeProvider> predicate) {
        if (predicate == null) {
            throw new IllegalArgumentException("predicate is null");
        }
        this.retryPredicate = predicate;
    }

    /**
     * set tm option listener
     *
     * @param listener tm option listener
     */
    public void setTmOptionListener(@Nullable TgTmOptionListener listener) {
        this.tmOptionListener = listener;
    }

    /**
     * get tm option
     *
     * @param attempt     attempt number
     * @param transaction transaction (null if attempt==0)
     * @param e           transaction exception (null if attempt==0)
     * @return Transaction Option
     */
    @Nonnull
    public final TgTmTxOption get(int attempt, TsurugiTransaction transaction, TsurugiTransactionException e) {
        var tmOption = computeTmOption(attempt, transaction, e);
        if (this.tmOptionListener != null) {
            tmOptionListener.accept(attempt, e, tmOption);
        }
        return tmOption;
    }

    protected TgTmTxOption computeTmOption(int attempt, TsurugiTransaction transaction, TsurugiTransactionException e) {
        if (attempt == 0) {
            return computeFirstTmOption();
        }

        if (isRetryable(transaction, e)) {
            return computeRetryTmOption(attempt, e);
        }

        return TgTmTxOption.notRetryable();
    }

    protected abstract TgTmTxOption computeFirstTmOption();

    protected abstract TgTmTxOption computeRetryTmOption(int attempt, TsurugiTransactionException e);

    /**
     * whether to retry
     *
     * @param transaction transaction
     * @param e           Transaction Exception
     * @return true: retryable
     */
    protected boolean isRetryable(TsurugiTransaction transaction, TsurugiTransactionException e) {
        return retryPredicate.test(transaction, e);
    }
}
