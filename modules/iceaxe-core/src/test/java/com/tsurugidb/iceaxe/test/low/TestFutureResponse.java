package com.tsurugidb.iceaxe.test.low;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.tsurugidb.iceaxe.util.TgTimeValue;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

public class TestFutureResponse<V> extends TestServerResource implements FutureResponse<V> {

    private TgTimeValue expectedTimeout;
    private boolean throwTimeout = false;

    public void setExpectedTimeout(long timeout, TimeUnit unit) {
        this.expectedTimeout = TgTimeValue.of(timeout, unit);
    }

    public void setThrowTimeout(boolean t) {
        this.throwTimeout = t;
    }

    @Override
    public boolean isDone() {
        throw new AssertionError("do override");
    }

    @Override
    public V get() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
        if (this.expectedTimeout != null) {
            long expected = expectedTimeout.toNanos();
            long actual = unit.toNanos(timeout);
            if (1 <= actual && actual <= expected) {
                // success
            } else {
                fail(String.format("timeout value error expected=%s(%d), actual=%d%s(%d)", //
                        expectedTimeout, expected, //
                        timeout, unit, actual));
            }
        }

        if (this.throwTimeout) {
//          unit.sleep(timeout);
            throw new TimeoutException("TestFutureResponse.get() timeout test");
        }

        return getInternal();
    }

    protected V getInternal() {
        return null; // do override
    }
}
