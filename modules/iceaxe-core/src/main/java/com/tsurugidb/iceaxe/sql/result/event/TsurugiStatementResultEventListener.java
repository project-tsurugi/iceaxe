package com.tsurugidb.iceaxe.sql.result.event;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;

/**
 * {@link TsurugiStatementResult} event listener
 */
public interface TsurugiStatementResultEventListener {

    /**
     * called when execute end
     *
     * @param result   SQL result
     * @param occurred exception
     */
    default void endResult(TsurugiStatementResult result, @Nullable Throwable occurred) {
        // do override
    }

    /**
     * called when close result
     *
     * @param result   SQL result
     * @param occurred exception
     */
    default void closeResult(TsurugiStatementResult result, @Nullable Throwable occurred) {
        // do override
    }
}
