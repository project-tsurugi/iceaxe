package com.tsurugidb.iceaxe.util;

/**
 * AutoCloseable with timeout.
 *
 * @since 1.4.0
 */
@FunctionalInterface
@IceaxeInternal
public interface IceaxeTimeoutCloseable extends AutoCloseable {

    @Override
    public default void close() throws Exception {
        throw new AssertionError("do override");
    }

    /**
     * Closes this resource.
     *
     * @param timeoutNanos close timeout
     * @throws Exception if this resource cannot be closed
     */
    public void close(long timeoutNanos) throws Exception;
}
