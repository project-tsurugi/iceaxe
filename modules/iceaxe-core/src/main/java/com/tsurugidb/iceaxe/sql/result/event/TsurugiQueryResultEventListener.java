package com.tsurugidb.iceaxe.sql.result.event;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;

/**
 * {@link TsurugiQueryResult} event listener
 */
public interface TsurugiQueryResultEventListener<R> {

    /**
     * called when read a record
     *
     * @param result SQL result
     * @param record record
     */
    default void readRecord(TsurugiQueryResult<R> result, R record) {
        // do override
    }

    /**
     * called when occurs exception
     *
     * @param result   SQL result
     * @param occurred exception
     */
    default void readException(TsurugiQueryResult<R> result, Throwable occurred) {
        // do override
    }

    /**
     * called when execute end
     *
     * @param result SQL result
     */
    default void endResult(TsurugiQueryResult<R> result) {
        // do override
    }

    /**
     * called when close result
     *
     * @param result   SQL result
     * @param occurred exception
     */
    default void closeResult(TsurugiQueryResult<R> result, @Nullable Throwable occurred) {
        // do override
    }
}
