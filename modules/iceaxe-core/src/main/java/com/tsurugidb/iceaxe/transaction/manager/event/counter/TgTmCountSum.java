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

import java.util.stream.Stream;

import javax.annotation.Nullable;

/**
 * sum of {@link TgTmCount}.
 */
public class TgTmCountSum implements TgTmCount {

    /**
     * Creates a new instance.
     *
     * @param stream counters
     * @return instance, {@code null} if stream is empty
     */
    @Nullable
    public static TgTmCountSum of(Stream<? extends TgTmCount> stream) {
        TgTmCountSum[] sum = { null };
        stream.forEach(count -> {
            if (sum[0] == null) {
                sum[0] = new TgTmCountSum();
            }
            sum[0].add(count);
        });
        return sum[0];
    }

    private int executeCount = 0;
    private int transactionCount = 0;
    private int exceptionCount = 0;
    private int retryCount = 0;
    private int retryOverCount = 0;
    private int beforeCommitCount = 0;
    private int commitCount = 0;
    private int rollbackCount = 0;
    private int successCommitCount = 0;
    private int successRollbackCount = 0;
    private int failCount = 0;

    /**
     * add count.
     *
     * @param count count
     */
    public void add(TgTmCount count) {
        executeCount += count.executeCount();
        transactionCount += count.transactionCount();
        exceptionCount += count.exceptionCount();
        retryCount += count.retryCount();
        retryOverCount += count.retryOverCount();
        beforeCommitCount += count.beforeCommitCount();
        commitCount += count.commitCount();
        rollbackCount += count.rollbackCount();
        successCommitCount += count.successCommitCount();
        successRollbackCount += count.successRollbackCount();
        failCount += count.failCount();
    }

    @Override
    public int executeCount() {
        return this.executeCount;
    }

    @Override
    public int transactionCount() {
        return this.transactionCount;
    }

    @Override
    public int exceptionCount() {
        return this.exceptionCount;
    }

    @Override
    public int retryCount() {
        return this.retryCount;
    }

    @Override
    public int retryOverCount() {
        return this.retryOverCount;
    }

    @Override
    public int beforeCommitCount() {
        return this.beforeCommitCount;
    }

    @Override
    public int commitCount() {
        return this.commitCount;
    }

    @Override
    public int rollbackCount() {
        return this.rollbackCount;
    }

    @Override
    public int successCommitCount() {
        return this.successCommitCount;
    }

    @Override
    public int successRollbackCount() {
        return this.successRollbackCount;
    }

    @Override
    public int failCount() {
        return this.failCount;
    }

    @Override
    public String toString() {
        return "[executeCount=" + executeCount + ", transactionCount=" + transactionCount + ", exceptionCount=" + exceptionCount + ", retryCount=" + retryCount + ", retryOverCount=" + retryOverCount
                + ", beforeCommitCount=" + beforeCommitCount + ", commitCount=" + commitCount + ", rollbackCount=" + rollbackCount + ", successCommitCount=" + successCommitCount
                + ", successRollbackCount=" + successRollbackCount + ", failCount=" + failCount + "]";
    }
}
