package com.tsurugidb.iceaxe.transaction.event;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.sql.TsurugiSql;
import com.tsurugidb.iceaxe.sql.result.TsurugiSqlResult;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction.TgTxMethod;

/**
 * {@link TsurugiTransaction} event listener
 */
public interface TsurugiTransactionEventListener {

    /**
     * called when low transaction get start
     *
     * @param transaction transaction
     */
    default void lowTransactionGetStart(TsurugiTransaction transaction) {
        // do override
    }

    /**
     * called when low transaction get end
     *
     * @param transaction   transaction
     * @param transactionId transactionId
     * @param occurred      exception
     */
    default void lowTransactionGetEnd(TsurugiTransaction transaction, @Nullable String transactionId, @Nullable Throwable occurred) {
        // do override
    }

    /**
     * called when execute start
     *
     * @param transaction       transaction
     * @param method            execute method
     * @param iceaxeTxExecuteId iceaxe tx executeId
     * @param ps                SQL definition
     * @param parameter         SQL parameter
     */
    default void executeStart(TsurugiTransaction transaction, TgTxMethod method, int iceaxeTxExecuteId, TsurugiSql ps, @Nullable Object parameter) {
        // do override
    }

    /**
     * called when execute end
     *
     * @param transaction       transaction
     * @param method            execute method
     * @param iceaxeTxExecuteId iceaxe tx executeId
     * @param ps                SQL definition
     * @param parameter         SQL parameter
     * @param result            SQL result
     * @param occurred          exception
     */
    default void executeEnd(TsurugiTransaction transaction, TgTxMethod method, int iceaxeTxExecuteId, TsurugiSql ps, @Nullable Object parameter, @Nullable TsurugiSqlResult result,
            @Nullable Throwable occurred) {
        // do override
    }

    /**
     * called when commit start
     *
     * @param transaction transaction
     * @param commitType  commit type
     */
    default void commitStart(TsurugiTransaction transaction, TgCommitType commitType) {
        // do override
    }

    /**
     * called when commit end
     *
     * @param transaction transaction
     * @param commitType  commit type
     * @param occurred    exception
     */
    default void commitEnd(TsurugiTransaction transaction, TgCommitType commitType, @Nullable Throwable occurred) {
        // do override
    }

    /**
     * called when rollback start
     *
     * @param transaction transaction
     */
    default void rollbackStart(TsurugiTransaction transaction) {
        // do override
    }

    /**
     * called when rollback end
     *
     * @param transaction transaction
     * @param occurred    exception
     */
    default void rollbackEnd(TsurugiTransaction transaction, @Nullable Throwable occurred) {
        // do override
    }

    /**
     * called when close transaction
     *
     * @param transaction transaction
     * @param occurred    exception
     */
    default void closeTransaction(TsurugiTransaction transaction, @Nullable Throwable occurred) {
        // do override
    }
}
