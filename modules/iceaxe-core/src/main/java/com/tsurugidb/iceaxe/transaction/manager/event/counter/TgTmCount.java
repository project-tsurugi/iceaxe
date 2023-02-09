package com.tsurugidb.iceaxe.transaction.manager.event.counter;

import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;

/**
 * count of processed {@link TsurugiTransactionManager}
 */
public interface TgTmCount {

    /**
     * get execute count
     *
     * @return execute count
     */
    int executeCount();

    /**
     * get transaction count
     *
     * @return transaction count
     */
    int transactionCount();

    /**
     * get exception count
     *
     * @return exception count
     */
    int execptionCount();

    /**
     * get retry count
     *
     * @return retry count
     */
    int retryCount();

    /**
     * get retry-over count
     *
     * @return retry-over count
     */
    int retryOverCount();

    /**
     * get retryable-abort count
     *
     * @return retryable-abort count
     */
    default int retryableAbortCount() {
        return retryCount() + retryOverCount();
    }

    /**
     * get before-commit count
     *
     * @return before-commit count
     */
    int beforeCommitCount();

    /**
     * get commit count
     *
     * @return commit count
     */
    int commitCount();

    /**
     * get rollback count
     *
     * @return rollback count
     */
    int rollbackCount();

    /**
     * get success(commit) count
     *
     * @return success(commit) count
     */
    int successCommitCount();

    /**
     * get success(rollback) count
     *
     * @return success(rollback) count
     */
    int successRollbackCount();

    /**
     * get success count
     *
     * @return success count
     */
    default int successCount() {
        return successCommitCount() + successRollbackCount();
    }

    /**
     * get fail count
     *
     * @return fail count
     */
    int failCount();
}
