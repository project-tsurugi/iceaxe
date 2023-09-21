package com.tsurugidb.iceaxe.transaction.manager.option;

import java.io.IOException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.retry.TgTmRetryInstruction;
import com.tsurugidb.iceaxe.transaction.manager.retry.TsurugiDefaultRetryPredicate;
import com.tsurugidb.iceaxe.transaction.manager.retry.TsurugiTmRetryPredicate;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * {@link TgTxOption} supplier.
 */
public abstract class TgTmTxOptionSupplier {

    /**
     * create TgTmTxOptionSupplier.
     *
     * @param txOptions transaction options
     * @return supplier
     */
    public static TgTmTxOptionSupplier of(TgTxOption... txOptions) {
        return TgTmTxOptionList.of(txOptions);
    }

    /**
     * create TgTmTxOptionSupplier.
     *
     * @param txOption transaction option
     * @return supplier
     */
    public static TgTmTxOptionSupplier ofAlways(TgTxOption txOption) {
        return TgTmTxOptionAlways.of(txOption, Integer.MAX_VALUE);
    }

    /**
     * create TgTmTxOptionSupplier.
     *
     * @param txOption        transaction option
     * @param attemptMaxCount attempt max count
     * @return supplier
     */
    public static TgTmTxOptionSupplier ofAlways(TgTxOption txOption, int attemptMaxCount) {
        return TgTmTxOptionAlways.of(txOption, attemptMaxCount);
    }

    /**
     * create TgTmTxOptionSupplier.
     *
     * @param txOption transaction option
     * @param size     size
     * @return supplier
     */
    public static TgTmTxOptionSupplier of(TgTxOption txOption, int size) {
        return TgTmTxOptionMultipleList.of().add(txOption, size);
    }

    /**
     * create TgTmTxOptionSupplier.
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
     * create TgTmTxOptionSupplier.
     *
     * @param occSize   occ size
     * @param ltxOption transaction option for LTX or RTX
     * @param ltxSize   ltx size
     * @return supplier
     */
    public static TgTmTxOptionSupplier ofOccLtx(int occSize, TgTxOption ltxOption, int ltxSize) {
        return TgTmTxOptionOccLtx.of(TgTxOption.ofOCC(), occSize, ltxOption, ltxSize);
    }

    /**
     * create TgTmTxOptionSupplier.
     *
     * @param occOption transaction option for OCC
     * @param occSize   occ size
     * @param ltxOption transaction option for LTX or RTX
     * @param ltxSize   ltx size
     * @return supplier
     */
    public static TgTmTxOptionSupplier ofOccLtx(TgTxOption occOption, int occSize, TgTxOption ltxOption, int ltxSize) {
        return TgTmTxOptionOccLtx.of(occOption, occSize, ltxOption, ltxSize);
    }

    /**
     * {@link TgTmTxOption} listener.
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

    private TsurugiTmRetryPredicate retryPredicate;
    private TgTmOptionListener tmOptionListener;
    private String description;

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
    public TgTmTxOptionSupplier(TsurugiTmRetryPredicate predicate) {
        setRetryPredicate(predicate);
    }

    /**
     * set retry predicate.
     *
     * @param predicate retry predicate
     */
    public void setRetryPredicate(@Nonnull TsurugiTmRetryPredicate predicate) {
        if (predicate == null) {
            throw new IllegalArgumentException("predicate is null");
        }
        this.retryPredicate = predicate;
    }

    /**
     * get retry predicate.
     *
     * @return retry predicate
     */
    public TsurugiTmRetryPredicate getRetryPredicate() {
        return this.retryPredicate;
    }

    /**
     * set tm option listener.
     *
     * @param listener tm option listener
     */
    public void setTmOptionListener(@Nullable TgTmOptionListener listener) {
        this.tmOptionListener = listener;
    }

    /**
     * create execute information.
     *
     * @param iceaxeTmExecuteId iceaxe tm executeId
     * @return execute information
     */
    public Object createExecuteInfo(int iceaxeTmExecuteId) {
        return null; // do override
    }

    /**
     * get transaction option.
     *
     * @param executeInfo {@link #createExecuteInfo(int)}
     * @param attempt     attempt number
     * @param transaction transaction (null if attempt==0)
     * @param e           transaction exception (null if attempt==0)
     * @return transaction option
     * @throws IOException
     * @throws InterruptedException
     */
    public final @Nonnull TgTmTxOption get(Object executeInfo, int attempt, TsurugiTransaction transaction, TsurugiTransactionException e) throws IOException, InterruptedException {
        var tmOption = computeTmOption(executeInfo, attempt, transaction, e);
        if (this.tmOptionListener != null) {
            tmOptionListener.accept(attempt, e, tmOption);
        }
        return tmOption;
    }

    /**
     * get transaction option.
     *
     * @param executeInfo {@link #createExecuteInfo(int)}
     * @param attempt     attempt number
     * @param transaction transaction (null if attempt==0)
     * @param e           transaction exception (null if attempt==0)
     * @return transaction option
     * @throws IOException
     * @throws InterruptedException
     */
    protected TgTmTxOption computeTmOption(Object executeInfo, int attempt, TsurugiTransaction transaction, TsurugiTransactionException e) throws IOException, InterruptedException {
        if (attempt == 0) {
            return computeFirstTmOption(executeInfo);
        }

        var retryInstruction = isRetryable(transaction, e);
        if (retryInstruction.isRetryable()) {
            return computeRetryTmOption(executeInfo, attempt, e, retryInstruction);
        }

        return TgTmTxOption.notRetryable(retryInstruction);
    }

    /**
     * get first transaction option.
     *
     * @param executeInfo {@link #createExecuteInfo(int)}
     * @return transaction option
     */
    protected abstract TgTmTxOption computeFirstTmOption(Object executeInfo);

    /**
     * get retry transaction option.
     *
     * @param executeInfo      {@link #createExecuteInfo(int)}
     * @param attempt          attempt number
     * @param e                transaction exception
     * @param retryInstruction retry instruction
     * @return transaction option
     */
    protected abstract TgTmTxOption computeRetryTmOption(Object executeInfo, int attempt, TsurugiTransactionException e, TgTmRetryInstruction retryInstruction);

    /**
     * whether to retry.
     *
     * @param transaction transaction
     * @param e           Transaction Exception
     * @return retry instruction
     * @throws IOException
     * @throws InterruptedException
     */
    protected TgTmRetryInstruction isRetryable(TsurugiTransaction transaction, TsurugiTransactionException e) throws IOException, InterruptedException {
        return getRetryPredicate().apply(transaction, e);
    }

    /**
     * set description.
     *
     * @param description description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * get description.
     *
     * @return description
     */
    public String getDescription() {
        if (this.description != null) {
            return this.description;
        }
        return getDefaultDescription();
    }

    /**
     * get default description.
     *
     * @return description
     */
    protected String getDefaultDescription() {
        // do override
        return getClass().getSimpleName();
    }

    @Override
    public String toString() {
        return getDescription();
    }
}
