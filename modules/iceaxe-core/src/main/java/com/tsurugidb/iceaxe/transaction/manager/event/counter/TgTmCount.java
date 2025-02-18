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

import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;

/**
 * count of processed {@link TsurugiTransactionManager}.
 */
public interface TgTmCount {

    /**
     * get execute count.
     *
     * @return execute count
     */
    int executeCount();

    /**
     * get transaction count.
     *
     * @return transaction count
     */
    int transactionCount();

    /**
     * get exception count.
     *
     * @return exception count
     */
    int exceptionCount();

    /**
     * get retry count.
     *
     * @return retry count
     */
    int retryCount();

    /**
     * get retry-over count.
     *
     * @return retry-over count
     */
    int retryOverCount();

    /**
     * get retryable-abort count.
     *
     * @return retryable-abort count
     */
    default int retryableAbortCount() {
        return retryCount() + retryOverCount();
    }

    /**
     * get before-commit count.
     *
     * @return before-commit count
     */
    int beforeCommitCount();

    /**
     * get commit count.
     *
     * @return commit count
     */
    int commitCount();

    /**
     * get rollback count.
     *
     * @return rollback count
     */
    int rollbackCount();

    /**
     * get success(commit) count.
     *
     * @return success(commit) count
     */
    int successCommitCount();

    /**
     * get success(rollback) count.
     *
     * @return success(rollback) count
     */
    int successRollbackCount();

    /**
     * get success count.
     *
     * @return success count
     */
    default int successCount() {
        return successCommitCount() + successRollbackCount();
    }

    /**
     * get fail count.
     *
     * @return fail count
     */
    int failCount();
}
