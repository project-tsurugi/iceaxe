package com.tsurugidb.iceaxe.transaction.manager;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.event.TsurugiTmEventListener;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOption;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOptionList;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOptionSupplier;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.util.TgTimeValue;

/**
 * Tsurugi Transaction Manager Settings
 *
 * @see TsurugiTransactionManager
 */
public class TgTmSetting {

    /**
     * create Transaction Manager Settings
     *
     * @param txOptionSupplier transaction option supplier
     * @returnTransaction Manager Settings
     */
    public static TgTmSetting of(TgTmTxOptionSupplier txOptionSupplier) {
        return new TgTmSetting().txOptionSupplier(txOptionSupplier);
    }

    /**
     * create Transaction Manager Settings
     *
     * @param txOptions transaction options
     * @return Transaction Manager Settings
     */
    public static TgTmSetting of(TgTxOption... txOptions) {
        var supplier = TgTmTxOptionList.of(txOptions);
        return of(supplier);
    }

    /**
     * create Transaction Manager Settings
     *
     * @param txOption transaction option
     * @return Transaction Manager Settings
     */
    public static TgTmSetting ofAlways(TgTxOption txOption) {
        var supplier = TgTmTxOptionSupplier.ofAlways(txOption);
        return of(supplier);
    }

    /**
     * create Transaction Manager Settings
     *
     * @param txOption       transaction option
     * @param attemtMaxCount attempt max count
     * @return Transaction Manager Settings
     */
    public static TgTmSetting ofAlways(TgTxOption txOption, int attemtMaxCount) {
        var supplier = TgTmTxOptionSupplier.ofAlways(txOption, attemtMaxCount);
        return of(supplier);
    }

    /**
     * create Transaction Manager Settings
     *
     * @param txOption transaction option
     * @param size     size
     * @return Transaction Manager Settings
     */
    public static TgTmSetting of(TgTxOption txOption, int size) {
        var supplier = TgTmTxOptionSupplier.of(txOption, size);
        return of(supplier);
    }

    /**
     * create Transaction Manager Settings
     *
     * @param txOption1 transaction option
     * @param size1     size
     * @param txOption2 transaction option
     * @param size2     size
     * @return Transaction Manager Settings
     */
    public static TgTmSetting of(TgTxOption txOption1, int size1, TgTxOption txOption2, int size2) {
        var supplier = TgTmTxOptionSupplier.of(txOption1, size1, txOption2, size2);
        return of(supplier);
    }

    private TgTmTxOptionSupplier txOptionSupplier;
    private String transactionLabel = null;
    private TgCommitType commitType;
    private TgTimeValue beginTimeout;
    private TgTimeValue commitTimeout;
    private TgTimeValue rollbackTimeout;
    private List<TsurugiTmEventListener> eventListenerList = null;

    /**
     * Tsurugi Transaction Manager Settings
     */
    public TgTmSetting() {
        // do nothing
    }

    /**
     * set transaction option supplier
     *
     * @param txOptionSupplier transaction option supplier
     * @return this
     */
    public TgTmSetting txOptionSupplier(TgTmTxOptionSupplier txOptionSupplier) {
        this.txOptionSupplier = txOptionSupplier;
        return this;
    }

    /**
     * get transaction option supplier
     *
     * @return transaction option supplier
     */
    public TgTmTxOptionSupplier getTransactionOptionSupplier() {
        return this.txOptionSupplier;
    }

    /**
     * set transaction label
     *
     * @param label label
     * @return this
     */
    public TgTmSetting transactionLabel(String label) {
        this.transactionLabel = label;
        return this;
    }

    /**
     * get transaction label
     *
     * @return label
     */
    public String transactionLabel() {
        return this.transactionLabel;
    }

    /**
     * get first transaction option
     *
     * @return transaction option
     */
    public TgTxOption getFirstTransactionOption() {
        var tmOption = getTransactionOption(0, null, null);
        var txOption = tmOption.getTransactionOption();
        if (txOption == null) {
            throw new IllegalStateException(MessageFormat.format("tmOption is not execute. tmOption={0}", tmOption));
        }
        if (txOption.label() == null && this.transactionLabel != null) {
            return txOption.clone(transactionLabel);
        }
        return txOption;
    }

    /**
     * get transaction option
     *
     * @param attempt     attempt number
     * @param transaction transaction
     * @param e           transaction exception
     * @return tm option
     * @see TgTmTxOptionSupplier
     */
    public TgTmTxOption getTransactionOption(int attempt, TsurugiTransaction transaction, TsurugiTransactionException e) {
        if (this.txOptionSupplier == null) {
            throw new IllegalStateException("txOptionSupplier is not specifed");
        }
        var tmOption = txOptionSupplier.get(attempt, transaction, e);
        if (tmOption.isExecute()) {
            var txOption = tmOption.getTransactionOption();
            if (txOption.label() == null && this.transactionLabel != null) {
                tmOption = TgTmTxOption.execute(txOption.clone(transactionLabel));
            }
        }
        return tmOption;
    }

    /**
     * set commit type
     *
     * @param commitType commit type
     */
    public void setCommitType(TgCommitType commitType) {
        this.commitType = commitType;
    }

    /**
     * set commit type
     *
     * @param commitType commit type
     * @return this
     */
    public TgTmSetting commitType(TgCommitType commitType) {
        setCommitType(commitType);
        return this;
    }

    /**
     * get commit type
     *
     * @param sessionOption session option
     * @return commit type
     */
    public TgCommitType getCommitType(TgSessionOption sessionOption) {
        if (this.commitType != null) {
            return this.commitType;
        }
        return sessionOption.getCommitType();
    }

    /**
     * set transaction-begin-timeout
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setBeginTimeout(long time, TimeUnit unit) {
        setBeginTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set transaction-begin-timeout
     *
     * @param timeout time
     */
    public void setBeginTimeout(TgTimeValue timeout) {
        this.beginTimeout = timeout;
    }

    /**
     * set transaction-begin-timeout
     *
     * @param time timeout time
     * @param unit timeout unit
     * @return this
     */
    public TgTmSetting beginTimeout(long time, TimeUnit unit) {
        setBeginTimeout(time, unit);
        return this;
    }

    /**
     * set transaction-begin-timeout
     *
     * @param timeout time
     * @return this
     */
    public TgTmSetting beginTimeout(TgTimeValue timeout) {
        setBeginTimeout(timeout);
        return this;
    }

    /**
     * set transaction-commit-timeout
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setCommitTimeout(long time, TimeUnit unit) {
        setCommitTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set transaction-commit-timeout
     *
     * @param timeout time
     */
    public void setCommitTimeout(TgTimeValue timeout) {
        this.commitTimeout = timeout;
    }

    /**
     * set transaction-commit-timeout
     *
     * @param time timeout time
     * @param unit timeout unit
     * @return this
     */
    public TgTmSetting commitTimeout(long time, TimeUnit unit) {
        setCommitTimeout(time, unit);
        return this;
    }

    /**
     * set transaction-commit-timeout
     *
     * @param timeout time
     * @return this
     */
    public TgTmSetting commitTimeout(TgTimeValue timeout) {
        setCommitTimeout(timeout);
        return this;
    }

    /**
     * set transaction-rollback-timeout
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setRollbackTimeout(long time, TimeUnit unit) {
        setRollbackTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set transaction-rollback-timeout
     *
     * @param timeout time
     */
    public void setRollbackTimeout(TgTimeValue timeout) {
        this.rollbackTimeout = timeout;
    }

    /**
     * set transaction-rollback-timeout
     *
     * @param time timeout time
     * @param unit timeout unit
     * @return this
     */
    public TgTmSetting rollbackTimeout(long time, TimeUnit unit) {
        setRollbackTimeout(time, unit);
        return this;
    }

    /**
     * set transaction-rollback-timeout
     *
     * @param timeout time
     * @return this
     */
    public TgTmSetting rollbackTimeout(TgTimeValue timeout) {
        setRollbackTimeout(timeout);
        return this;
    }

    /**
     * add event listener
     *
     * @param listener event listener
     * @return this
     */
    public TgTmSetting addEventListener(TsurugiTmEventListener listener) {
        if (this.eventListenerList == null) {
            this.eventListenerList = new ArrayList<>();
        }
        eventListenerList.add(listener);
        return this;
    }

    /**
     * get event listener
     *
     * @return event listener
     */
    public @Nullable List<TsurugiTmEventListener> getEventListener() {
        return this.eventListenerList;
    }

    // internal
    public void initializeTransaction(TsurugiTransaction transaction) {
        if (beginTimeout != null) {
            transaction.setBeginTimeout(beginTimeout);
        }
        if (commitTimeout != null) {
            transaction.setCommitTimeout(commitTimeout);
        }
        if (rollbackTimeout != null) {
            transaction.setRollbackTimeout(rollbackTimeout);
        }
    }
}
