package com.tsurugidb.iceaxe.transaction.event;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.statement.TsurugiSql;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * {@link TsurugiTransaction} event listener
 */
public interface TsurugiTransactionEventListener {

    /**
     * called when get transactionId
     *
     * @param transaction   transaction
     * @param transactionId transactionId
     */
    default void gotTransactionId(TsurugiTransaction transaction, String transactionId) {
        // do override
    }

    /**
     * called when execute start
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param parameter   SQL parameter
     */
    default void executeStart(TsurugiTransaction transaction, TsurugiSql ps, @Nullable Object parameter) {
        // do override
    }

    /**
     * called when execute end
     *
     * @param transaction transaction
     * @param ps          SQL statement
     * @param parameter   SQL parameter
     * @param occurred    exception
     */
    default void executeEnd(TsurugiTransaction transaction, TsurugiSql ps, @Nullable Object parameter, @Nullable Throwable occurred) {
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
