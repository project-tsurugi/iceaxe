package com.tsurugidb.iceaxe.util;

import java.util.concurrent.TimeUnit;

import com.nautilus_technologies.tsubakuro.util.ServerResource;
import com.nautilus_technologies.tsubakuro.util.Timeout;
import com.nautilus_technologies.tsubakuro.util.Timeout.Policy;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;

// internal
public class IceaxeTimeout {

    private final TgSessionInfo info;
    private final TgTimeoutKey key;
    private TgTimeValue value;

    public IceaxeTimeout(TgSessionInfo info, TgTimeoutKey key) {
        this.info = info;
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
            this.value = info.timeout(key);
        }
        return this.value;
    }

    public void apply(ServerResource target) {
        if (target != null) {
            var timeout = get();
            var lowTimeout = new Timeout(timeout.value(), timeout.unit(), Policy.ERROR);
            target.setCloseTimeout(lowTimeout);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{key=" + key + ", value=" + value + "}";
    }
}
