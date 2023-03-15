package com.tsurugidb.iceaxe.sql.result.event;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.sql.result.TusurigQueryResult;

/**
 * {@link TusurigQueryResult} event listener
 */
public interface TsurugiResultSetEventListener<R> {

    /**
     * called when read a record
     *
     * @param rs     ResultSet
     * @param record record
     */
    default void readRecord(TusurigQueryResult<R> rs, R record) {
        // do override
    }

    /**
     * called when occurs exception
     *
     * @param rs       ResultSet
     * @param occurred exception
     */
    default void readException(TusurigQueryResult<R> rs, Throwable occurred) {
        // do override
    }

    /**
     * called when execute end
     *
     * @param rs ResultSet
     */
    default void endResult(TusurigQueryResult<R> rs) {
        // do override
    }

    /**
     * called when close ResultSet
     *
     * @param rs       ResultSet
     * @param occurred exception
     */
    default void closeResult(TusurigQueryResult<R> rs, @Nullable Throwable occurred) {
        // do override
    }
}
