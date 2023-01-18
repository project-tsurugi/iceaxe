package com.tsurugidb.iceaxe.transaction.manager.event.counter;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.manager.event.TgTmEventListener;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * {@link TsurugiTransactionManager} label counter
 */
@ThreadSafe
public class TgTmLabelCounter extends TgTmEventListener {

    private final Map<String, TgTmCountAtomic> counter = new ConcurrentHashMap<>();

    @Override
    public void executeStart(TgTxOption option) {
        String label = label(option);
        getOrCreate(label).incrementExecuteCount();
    }

    @Override
    public void transactionBefore(int attempt, TgTxOption option) {
        String label = label(option);
        getOrCreate(label).incrementTransactionCount();
    }

    @Override
    public void transactionCreated(TsurugiTransaction transaction) {
        transaction.addBeforeCommitListener(tx -> {
            String label = label(tx);
            getOrCreate(label).incrementBeforeCommitCount();
        });
        transaction.addCommitListener(tx -> {
            String label = label(tx);
            getOrCreate(label).incrementCommitCount();
        });
        transaction.addRollbackListener(tx -> {
            String label = label(tx);
            getOrCreate(label).incrementRollbackCount();
        });
    }

    @Override
    public void transactionException(TsurugiTransaction transaction, Throwable e) {
        String label = label(transaction);
        getOrCreate(label).incrementExceptionCount();
    }

    @Override
    public void transactionRetry(TsurugiTransaction transaction, Exception cause, TgTxOption nextOption) {
        String label = label(transaction);
        getOrCreate(label).incrementRetryCount();
    }

    @Override
    public void transactionRetryOver(TsurugiTransaction transaction, Exception cause) {
        String label = label(transaction);
        getOrCreate(label).incrementRetryOverCount();
    }

    @Override
    public void executeEndSuccess(TsurugiTransaction transaction, boolean committed, Object returnValue) {
        String label = label(transaction);
        if (committed) {
            getOrCreate(label).incrementSuccessCommitCount();
        } else {
            getOrCreate(label).incrementSuccessRollbackCount();
        }
    }

    @Override
    public void executeEndFail(TgTxOption option, TsurugiTransaction transaction, Throwable e) {
        String label;
        if (transaction != null) {
            label = label(transaction);
        } else {
            label = label(option);
        }
        getOrCreate(label).incrementFailCount();
    }

    protected String label(TsurugiTransaction transaction) {
        return label(transaction.getTransactionOption());
    }

    protected String label(TgTxOption option) {
        String label = option.label();
        return (label != null) ? label : "";
    }

    protected TgTmCountAtomic getOrCreate(String label) {
        return counter.computeIfAbsent(label, k -> new TgTmCountAtomic());
    }

    /**
     * get count map
     *
     * @return count map
     */
    public Map<String, ? extends TgTmCount> getCountMap() {
        return this.counter;
    }

    /**
     * get count
     *
     * @param label label
     * @return count
     */
    public Optional<TgTmCount> findCount(String label) {
        var count = counter.get(label);
        return Optional.ofNullable(count);
    }

    /**
     * get count
     *
     * @param labelPrefix label prefix
     * @return count
     */
    public Optional<TgTmCount> findCountByPrefix(String labelPrefix) {
        var count = TgTmCountSum.of(counter.entrySet().stream() //
                .filter(entry -> entry.getKey().startsWith(labelPrefix)) //
                .map(entry -> entry.getValue()));
        return Optional.ofNullable(count);
    }

    /**
     * get count
     *
     * @return count
     */
    public TgTmCount getCount() {
        var count = TgTmCountSum.of(counter.values().stream());
        return (count != null) ? count : new TgTmCountSum();
    }

    /**
     * reset count
     */
    public void reset() {
        counter.clear();
    }

    @Override
    public String toString() {
        return "TgTmLabelCounter" + counter;
    }
}
