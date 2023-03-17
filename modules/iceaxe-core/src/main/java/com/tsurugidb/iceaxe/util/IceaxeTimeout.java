package com.tsurugidb.iceaxe.util;

import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.Timeout;
import com.tsurugidb.tsubakuro.util.Timeout.Policy;

// internal
public class IceaxeTimeout {

    private final TgSessionOption sessionOption;
    private final TgTimeoutKey key;
    private TgTimeValue value;

    public IceaxeTimeout(TgSessionOption sessionOption, TgTimeoutKey key) {
        this.sessionOption = sessionOption;
        this.key = key;
    }

    public void set(long time, TimeUnit unit) {
        set(new TgTimeValue(time, unit));
    }

    public void set(TgTimeValue timeout) {
        this.value = timeout;
    }

    public TgTimeValue get() {
        if (this.value == null) {
            this.value = sessionOption.getTimeout(key);
        }
        return this.value;
    }

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
