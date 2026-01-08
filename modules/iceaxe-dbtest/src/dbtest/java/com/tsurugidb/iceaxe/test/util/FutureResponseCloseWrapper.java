/*
 * Copyright 2023-2026 Project Tsurugi.
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
package com.tsurugidb.iceaxe.test.util;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

public class FutureResponseCloseWrapper<V> implements FutureResponse<V> {

    public static <V> FutureResponseCloseWrapper<V> of(FutureResponse<V> future) {
        return new FutureResponseCloseWrapper<>(future);
    }

    private final FutureResponse<V> owner;
    private boolean closed = false;

    public FutureResponseCloseWrapper(FutureResponse<V> owner) {
        this.owner = owner;
    }

    @Override
    public boolean isDone() {
        return owner.isDone();
    }

    @Override
    public V get() throws IOException, ServerException, InterruptedException {
        return owner.get();
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
        return owner.get(timeout, unit);
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        this.closed = true;
        owner.close();
    }

    public boolean isClosed() {
        return this.closed;
    }
}
