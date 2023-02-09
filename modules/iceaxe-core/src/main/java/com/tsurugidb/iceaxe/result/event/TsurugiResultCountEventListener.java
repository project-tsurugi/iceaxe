package com.tsurugidb.iceaxe.result.event;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.result.TsurugiResultCount;

/**
 * {@link TsurugiResultCount} event listener
 */
public interface TsurugiResultCountEventListener {

    /**
     * called when execute end
     *
     * @param rc       ResultCount
     * @param occurred exception
     */
    default void endResult(TsurugiResultCount rc, @Nullable Throwable occurred) {
        // do override
    }

    /**
     * called when close ResultCount
     *
     * @param rc       ResultCount
     * @param occurred exception
     */
    default void closeResult(TsurugiResultCount rc, @Nullable Throwable occurred) {
        // do override
    }
}
