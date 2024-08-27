/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.iceaxe.util;

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.Immutable;

/**
 * Iceaxe time.
 */
@Immutable
public /* record */ class TgTimeValue {

    /**
     * create time.
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
     * Creates a new instance.
     *
     * @param value time value
     * @param unit  time unit
     */
    public TgTimeValue(long value, TimeUnit unit) {
        this.value = value;
        this.unit = unit;
    }

    /**
     * get time value.
     *
     * @return time value
     */
    public long value() {
        return this.value;
    }

    /**
     * get time unit.
     *
     * @return time unit
     */
    public TimeUnit unit() {
        return this.unit;
    }

    /**
     * get time value[nanosecond].
     *
     * @return time value
     * @since 1.4.0
     */
    public long toNanos() {
        return unit.toNanos(this.value);
    }

    @Override
    public String toString() {
        return value + unit.toString().toLowerCase();
    }
}
