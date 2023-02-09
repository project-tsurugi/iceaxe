package com.tsurugidb.iceaxe.result.event;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.result.TsurugiResultSet;

/**
 * {@link TsurugiResultSet} event listener
 */
public interface TsurugiResultSetEventListener<R> {

    /**
     * called when read a record
     *
     * @param rs     ResultSet
     * @param record record
     */
    default void readRecord(TsurugiResultSet<R> rs, R record) {
        // do override
    }

    /**
     * called when occurs exception
     *
     * @param rs       ResultSet
     * @param occurred exception
     */
    default void readException(TsurugiResultSet<R> rs, Throwable occurred) {
        // do override
    }

    /**
     * called when execute end
     *
     * @param rs ResultSet
     */
    default void endResult(TsurugiResultSet<R> rs) {
        // do override
    }

    /**
     * called when close ResultSet
     *
     * @param rs       ResultSet
     * @param occurred exception
     */
    default void closeResult(TsurugiResultSet<R> rs, @Nullable Throwable occurred) {
        // do override
    }
}
