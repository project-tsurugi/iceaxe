package com.tsurugi.iceaxe.util;

import java.util.concurrent.TimeUnit;

/**
 * time
 */
public class TgTimeValue { // record

    /**
     * create time
     * 
     * @param value time value
     * @param unit  time unit
     * @return time
     */
    public static TgTimeValue of(long value, TimeUnit unit) {
        return new TgTimeValue(value, unit);
    }

    private final long value;
    private final TimeUnit unit;

    /**
     * time
     * 
     * @param value time value
     * @param unit  time unit
     */
    public TgTimeValue(long value, TimeUnit unit) {
        this.value = value;
        this.unit = unit;
    }

    /**
     * get time value
     * 
     * @return time value
     */
    public long value() {
        return this.value;
    }

    /**
     * get time unit
     * 
     * @return time unit
     */
    public TimeUnit unit() {
        return this.unit;
    }

    @Override
    public String toString() {
        return value + unit.toString().toLowerCase();
    }
}
