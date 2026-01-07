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
