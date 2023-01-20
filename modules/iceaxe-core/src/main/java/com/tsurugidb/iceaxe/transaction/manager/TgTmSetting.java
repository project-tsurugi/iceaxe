package com.tsurugidb.iceaxe.transaction.manager;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.event.TgTmEventListener;
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
     * @param transactionOptionSupplier transaction option supplier
     * @returnTransaction Manager Settings
     */
    public static TgTmSetting of(TgTmTxOptionSupplier transactionOptionSupplier) {
        return new TgTmSetting().transactionOptionSupplier(transactionOptionSupplier);
    }

    /**
     * create Transaction Manager Settings
     *
     * @param transactionOptionList transaction option list
     * @return Transaction Manager Settings
     */
    public static TgTmSetting of(TgTxOption... transactionOptionList) {
        var supplier = TgTmTxOptionList.of(transactionOptionList);
        return of(supplier);
    }

    /**
     * create Transaction Manager Settings
     *
     * @param transactionOption transaction option
     * @return Transaction Manager Settings
     */
    public static TgTmSetting ofAlways(TgTxOption transactionOption) {
        return ofAlways(transactionOption, Integer.MAX_VALUE);
    }

    /**
     * create Transaction Manager Settings
     *
     * @param transactionOption transaction option
     * @param attemtMaxCount    attempt max count
     * @return Transaction Manager Settings
     */
    public static TgTmSetting ofAlways(TgTxOption transactionOption, int attemtMaxCount) {
        var supplier = TgTmTxOptionSupplier.ofAlways(transactionOption, attemtMaxCount);
        return of(supplier);
    }

    private TgTmTxOptionSupplier transactionOptionSupplier;
    private String transactionLabel = null;
    private TgCommitType commitType;
    private TgTimeValue beginTimeout;
    private TgTimeValue commitTimeout;
    private TgTimeValue rollbackTimeout;
    private final List<TgTmEventListener> eventListenerList = new ArrayList<>();

    /**
     * Tsurugi Transaction Manager Settings
     */
    public TgTmSetting() {
        // do nothing
    }

    /**
     * set transaction option supplier
     *
     * @param transactionOptionSupplier transaction option supplier
     * @return this
     */
    public TgTmSetting transactionOptionSupplier(TgTmTxOptionSupplier transactionOptionSupplier) {
        this.transactionOptionSupplier = transactionOptionSupplier;
        return this;
    }

    /**
     * get transaction option supplier
     *
     * @return transaction option supplier
     */
    public TgTmTxOptionSupplier getTransactionOptionSupplier() {
        return this.transactionOptionSupplier;
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
        var option = tmOption.getOption();
        if (option == null) {
            throw new IllegalStateException(MessageFormat.format("tm-option is not execute. tmOption={0}", tmOption));
        }
        if (option.label() == null && this.transactionLabel != null) {
            return option.clone(transactionLabel);
        }
        return option;
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
        if (this.transactionOptionSupplier == null) {
            throw new IllegalStateException("transactionOptionSupplier is not specifed");
        }
        var tmOption = transactionOptionSupplier.get(attempt, transaction, e);
        if (tmOption.isExecute()) {
            var option = tmOption.getOption();
            if (option.label() == null && this.transactionLabel != null) {
                tmOption = TgTmTxOption.execute(option.clone(transactionLabel));
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
     * @param info Session information
     * @return commit type
     */
    public TgCommitType getCommitType(TgSessionInfo info) {
        if (this.commitType != null) {
            return this.commitType;
        }
        return info.commitType();
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
    public TgTmSetting addEventListener(TgTmEventListener listener) {
        eventListenerList.add(listener);
        return this;
    }

    /**
     * get event listener
     *
     * @return event listener
     */
    public List<TgTmEventListener> getEventListener() {
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
