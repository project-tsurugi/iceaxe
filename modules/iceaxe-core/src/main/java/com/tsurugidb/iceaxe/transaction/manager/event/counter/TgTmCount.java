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
    public int executeCount();

    /**
     * get transaction count
     *
     * @return transaction count
     */
    public int transactionCount();

    /**
     * get exception count
     *
     * @return exception count
     */
    public int execptionCount();

    /**
     * get retry count
     *
     * @return retry count
     */
    public int retryCount();

    /**
     * get retry-over count
     *
     * @return retry-over count
     */
    public int retryOverCount();

    /**
     * get retryable-abort count
     *
     * @return retryable-abort count
     */
    public default int retryableAbortCount() {
        return retryCount() + retryOverCount();
    }

    /**
     * get before-commit count
     *
     * @return before-commit count
     */
    public int beforeCommitCount();

    /**
     * get commit count
     *
     * @return commit count
     */
    public int commitCount();

    /**
     * get rollback count
     *
     * @return rollback count
     */
    public int rollbackCount();

    /**
     * get success(commit) count
     *
     * @return success(commit) count
     */
    public int successCommitCount();

    /**
     * get success(rollback) count
     *
     * @return success(rollback) count
     */
    public int successRollbackCount();

    /**
     * get success count
     *
     * @return success count
     */
    public default int successCount() {
        return successCommitCount() + successRollbackCount();
    }

    /**
     * get fail count
     *
     * @return fail count
     */
    public int failCount();
}
