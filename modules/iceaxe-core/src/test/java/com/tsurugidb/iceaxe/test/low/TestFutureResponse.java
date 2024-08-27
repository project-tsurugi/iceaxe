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

    protected V getInternal() throws IOException, ServerException, InterruptedException {
        return null; // do override
    }
}
