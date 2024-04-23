package com.tsurugidb.iceaxe.test.low;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.util.TgTimeValue;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.Timeout;

public class TestServerResource implements ServerResource {

    private TgTimeValue expectedCloseTimeout;
    private boolean throwCloseTimeout = false;

    private Timeout closeTimeout;
    private boolean closed = false;

    public void setExpectedCloseTimeout(long timeout, TimeUnit unit) {
        setExpectedCloseTimeout(TgTimeValue.of(timeout, unit));
    }

    public void setExpectedCloseTimeout(TgTimeValue timeout) {
        this.expectedCloseTimeout = timeout;
    }

    public void setThrowCloseTimeout(boolean t) {
        this.throwCloseTimeout = t;
    }

    @Override
    public void setCloseTimeout(Timeout timeout) {
        this.closeTimeout = timeout;
    }

    public Timeout getCloseTimeout() {
        return this.closeTimeout;
    }

    public long getCloseTimeoutNanos() {
        return closeTimeout.unit().toNanos(closeTimeout.value());
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        this.closed = true;

        if (this.expectedCloseTimeout != null) {
            long expected = expectedCloseTimeout.toNanos();
            long actual = getCloseTimeoutNanos();
            if (1 <= actual && actual <= expected) {
                // success
            } else {
                fail(String.format("timeout value error expected=%s(%d), actual=%d%s(%d)", //
                        expectedCloseTimeout, expected, //
                        closeTimeout.value(), closeTimeout.unit(), actual));
            }
        }

        if (this.throwCloseTimeout) {
//          unit.sleep(value);
            throw new ResponseTimeoutException("TestFutureResponse.close() timeout test");
        }
    }

    public boolean isClosed() {
        return this.closed;
    }
}
