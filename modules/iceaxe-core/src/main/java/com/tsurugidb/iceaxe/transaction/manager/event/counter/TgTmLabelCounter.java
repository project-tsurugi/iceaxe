/*
 * Copyright 2023-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.iceaxe.transaction.manager.event.counter;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.event.TsurugiTransactionEventListener;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.manager.event.TsurugiTmEventListener;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOption;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * {@link TsurugiTransactionManager} label counter.
 */
@ThreadSafe
public class TgTmLabelCounter implements TsurugiTmEventListener {

    private final Map<String, TgTmCountAtomic> counterMap = new ConcurrentHashMap<>();

    @Override
    public void executeStart(TsurugiTransactionManager tm, int iceaxeTmExecuteId, TgTxOption txOption) {
        String label = label(txOption);
        getOrCreate(label).incrementExecuteCount();
    }

    @Override
    public void transactionStart(TsurugiTransactionManager tm, int iceaxeTmExecuteId, int attempt, TgTxOption txOption) {
        String label = label(txOption);
        getOrCreate(label).incrementTransactionCount();
    }

    @Override
    public void transactionStarted(TsurugiTransaction transaction) {
        transaction.addEventListener(new TsurugiTransactionEventListener() {
            @Override
            public void commitStart(TsurugiTransaction transaction, TgCommitType commitType) {
                String label = label(transaction);
                getOrCreate(label).incrementBeforeCommitCount();
            }

            @Override
            public void commitEnd(TsurugiTransaction transaction, TgCommitType commitType, Throwable occurred) {
                if (occurred == null) {
                    String label = label(transaction);
                    getOrCreate(label).incrementCommitCount();
                }
            }

            @Override
            public void rollbackEnd(TsurugiTransaction transaction, Throwable occurred) {
                if (occurred == null) {
                    String label = label(transaction);
                    getOrCreate(label).incrementRollbackCount();
                }
            }
        });
    }

    @Override
    public void transactionException(TsurugiTransaction transaction, Throwable e) {
        String label = label(transaction);
        getOrCreate(label).incrementExceptionCount();
    }

    @Override
    public void transactionRetry(TsurugiTransaction transaction, Exception cause, TgTmTxOption nextTmOption) {
        String label = label(transaction);
        getOrCreate(label).incrementRetryCount();
    }

    @Override
    public void transactionRetryOver(TsurugiTransaction transaction, Exception cause, TgTmTxOption nextTmOption) {
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
    public void executeEndFail(TsurugiTransactionManager tm, int iceaxeTmExecuteId, TgTxOption txOption, TsurugiTransaction transaction, Throwable e) {
        String label;
        if (transaction != null) {
            label = label(transaction);
        } else {
            label = label(txOption);
        }
        getOrCreate(label).incrementFailCount();
    }

    /**
     * get transaction label.
     *
     * @param transaction transaction
     * @return transaction label
     */
    protected String label(TsurugiTransaction transaction) {
        return label(transaction.getTransactionOption());
    }

    /**
     * get transaction label.
     *
     * @param txOption transaction option
     * @return transaction label
     */
    protected String label(TgTxOption txOption) {
        String label = txOption.label();
        return (label != null) ? label : "";
    }

    /**
     * get or create count.
     *
     * @param label transaction label
     * @return count
     */
    protected TgTmCountAtomic getOrCreate(String label) {
        return counterMap.computeIfAbsent(label, this::newTmCountAtomic);
    }

    /**
     * Creates a new count instance.
     *
     * @param label transaction label
     * @return count
     */
    protected TgTmCountAtomic newTmCountAtomic(String label) {
        return new TgTmCountAtomic();
    }

    /**
     * get count map.
     *
     * @return count map
     */
    public Map<String, ? extends TgTmCount> getCountMap() {
        return this.counterMap;
    }

    /**
     * get count.
     *
     * @param label label
     * @return count
     */
    public Optional<TgTmCount> findCount(String label) {
        var count = counterMap.get(label);
        return Optional.ofNullable(count);
    }

    /**
     * get count.
     *
     * @param labelPrefix label prefix
     * @return count
     */
    public Optional<TgTmCount> findCountByPrefix(String labelPrefix) {
        var count = TgTmCountSum.of(counterMap.entrySet().stream() //
                .filter(entry -> entry.getKey().startsWith(labelPrefix)) //
                .map(entry -> entry.getValue()));
        return Optional.ofNullable(count);
    }

    /**
     * get count.
     *
     * @return count
     */
    public TgTmCount getCount() {
        var count = TgTmCountSum.of(counterMap.values().stream());
        return (count != null) ? count : new TgTmCountSum();
    }

    /**
     * reset count.
     */
    @OverridingMethodsMustInvokeSuper
    public void reset() {
        counterMap.clear();
    }

    @Override
    public String toString() {
        return "TgTmLabelCounter" + counterMap;
    }
}
