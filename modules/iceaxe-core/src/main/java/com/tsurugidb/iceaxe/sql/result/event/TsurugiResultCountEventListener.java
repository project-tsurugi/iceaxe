package com.tsurugidb.iceaxe.sql.result.event;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;

/**
 * {@link TsurugiStatementResult} event listener
 */
public interface TsurugiResultCountEventListener {

    /**
     * called when execute end
     *
     * @param rc       ResultCount
     * @param occurred exception
     */
    default void endResult(TsurugiStatementResult rc, @Nullable Throwable occurred) {
        // do override
    }

    /**
     * called when close ResultCount
     *
     * @param rc       ResultCount
     * @param occurred exception
     */
    default void closeResult(TsurugiStatementResult rc, @Nullable Throwable occurred) {
        // do override
    }
}
