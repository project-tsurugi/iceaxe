/*
 * Copyright 2023-2025 Project Tsurugi.
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

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.Timeout;
import com.tsurugidb.tsubakuro.util.Timeout.Policy;

/**
 * Iceaxe timeout.
 */
@IceaxeInternal
public class IceaxeTimeout {

    private final TgSessionOption sessionOption;
    private final TgTimeoutKey key;
    private TgTimeValue value;

    /**
     * Creates a new instance.
     *
     * @param sessionOption session option
     * @param key           timeout key
     */
    public IceaxeTimeout(TgSessionOption sessionOption, TgTimeoutKey key) {
        this.sessionOption = sessionOption;
        this.key = key;
    }

    /**
     * Creates a new instance.
     *
     * @param time time value
     * @param unit time unit
     * @since 1.4.0
     */
    public IceaxeTimeout(long time, TimeUnit unit) {
        this.sessionOption = null;
        this.key = null;
        this.value = new TgTimeValue(time, unit);
    }

    /**
     * set time.
     *
     * @param time time value
     * @param unit time unit
     */
    public void set(long time, TimeUnit unit) {
        set(new TgTimeValue(time, unit));
    }

    /**
     * set time.
     *
     * @param timeout time
     */
    public void set(TgTimeValue timeout) {
        this.value = timeout;
    }

    /**
     * get time.
     *
     * @return time
     */
    public TgTimeValue get() {
        if (this.value == null) {
            this.value = sessionOption.getTimeout(key);
        }
        return this.value;
    }

    /**
     * get time.
     *
     * @return time [nanosecond]
     * @since 1.4.0
     */
    public long getNanos() {
        return get().toNanos();
    }

    /**
     * apply time.
     *
     * @param target low server resource
     */
    public void apply(ServerResource target) {
        if (target != null) {
            var time = get();
            var lowTimeout = new Timeout(time.value(), time.unit(), Policy.ERROR);
            target.setCloseTimeout(lowTimeout);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{key=" + key + ", value=" + value + "}";
    }
}
