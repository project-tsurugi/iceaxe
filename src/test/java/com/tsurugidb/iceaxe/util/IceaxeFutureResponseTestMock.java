package com.tsurugidb.iceaxe.util;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

class IceaxeFutureResponseTestMock<V> implements FutureResponse<V> {

    @Override
    public boolean isDone() {
        fail();
        return false;
    }

    @Override
    public V get() throws IOException, ServerException, InterruptedException {
        fail();
        return null;
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
        fail();
        return null;
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        fail();
    }
}
