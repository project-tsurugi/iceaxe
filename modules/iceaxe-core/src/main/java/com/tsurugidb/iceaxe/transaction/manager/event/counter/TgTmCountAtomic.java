/*
 * Copyright 2023-2026 Project Tsurugi.
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

import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

/**
 * {@link TgTmCount} implemented by AtomicInteger.
 */
@ThreadSafe
public class TgTmCountAtomic implements TgTmCount {

    private final AtomicInteger executeCount = new AtomicInteger(0);
    private final AtomicInteger transactionCount = new AtomicInteger(0);
    private final AtomicInteger exceptionCount = new AtomicInteger(0);
    private final AtomicInteger retryCount = new AtomicInteger(0);
    private final AtomicInteger retryOverCount = new AtomicInteger(0);
    private final AtomicInteger beforeCommitCount = new AtomicInteger(0);
    private final AtomicInteger commitCount = new AtomicInteger(0);
    private final AtomicInteger rollbackCount = new AtomicInteger(0);
    private final AtomicInteger successCommitCount = new AtomicInteger(0);
    private final AtomicInteger successRollbackCount = new AtomicInteger(0);
    private final AtomicInteger failCount = new AtomicInteger(0);

    /**
     * increment execute count.
     */
    public void incrementExecuteCount() {
        executeCount.incrementAndGet();
    }

    /**
     * increment transaction count.
     */
    public void incrementTransactionCount() {
        transactionCount.incrementAndGet();
    }

    /**
     * increment exception count.
     */
    public void incrementExceptionCount() {
        exceptionCount.incrementAndGet();
    }

    /**
     * increment retry count.
     */
    public void incrementRetryCount() {
        retryCount.incrementAndGet();
    }

    /**
     * increment retry-over count.
     */
    public void incrementRetryOverCount() {
        retryOverCount.incrementAndGet();
    }

    /**
     * increment before-commit count.
     */
    public void incrementBeforeCommitCount() {
        beforeCommitCount.incrementAndGet();
    }

    /**
     * increment commit count.
     */
    public void incrementCommitCount() {
        commitCount.incrementAndGet();
    }

    /**
     * increment rollback count.
     */
    public void incrementRollbackCount() {
        rollbackCount.incrementAndGet();
    }

    /**
     * increment success(commit) count.
     */
    public void incrementSuccessCommitCount() {
        successCommitCount.incrementAndGet();
    }

    /**
     * increment success(rollback) count.
     */
    public void incrementSuccessRollbackCount() {
        successRollbackCount.incrementAndGet();
    }

    /**
     * increment fail count.
     */
    public void incrementFailCount() {
        failCount.incrementAndGet();
    }

    @Override
    public int executeCount() {
        return executeCount.get();
    }

    @Override
    public int transactionCount() {
        return transactionCount.get();
    }

    @Override
    public int exceptionCount() {
        return exceptionCount.get();
    }

    @Override
    public int retryCount() {
        return retryCount.get();
    }

    @Override
    public int retryOverCount() {
        return retryOverCount.get();
    }

    @Override
    public int beforeCommitCount() {
        return beforeCommitCount.get();
    }

    @Override
    public int commitCount() {
        return commitCount.get();
    }

    @Override
    public int rollbackCount() {
        return rollbackCount.get();
    }

    @Override
    public int successCommitCount() {
        return successCommitCount.get();
    }

    @Override
    public int successRollbackCount() {
        return successRollbackCount.get();
    }

    @Override
    public int failCount() {
        return failCount.get();
    }

    /**
     * clear count.
     */
    public void clear() {
        executeCount.set(0);
        transactionCount.set(0);
        exceptionCount.set(0);
        retryCount.set(0);
        retryOverCount.set(0);
        beforeCommitCount.set(0);
        commitCount.set(0);
        rollbackCount.set(0);
        successCommitCount.set(0);
        successRollbackCount.set(0);
        failCount.set(0);
    }

    @Override
    public String toString() {
        return "[executeCount=" + executeCount + ", transactionCount=" + transactionCount + ", exceptionCount=" + exceptionCount + ", retryCount=" + retryCount + ", retryOverCount=" + retryOverCount
                + ", beforeCommitCount=" + beforeCommitCount + ", commitCount=" + commitCount + ", rollbackCount=" + rollbackCount + ", successCommitCount=" + successCommitCount
                + ", successRollbackCount=" + successRollbackCount + ", failCount=" + failCount + "]";
    }
}
