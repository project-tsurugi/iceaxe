package com.tsurugidb.iceaxe.test.low;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.ResponseProcessor;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResource;

public class TestLowSession extends TestServerResource implements Session {

    @Override
    public <R> FutureResponse<R> send(int serviceId, byte[] payload, ResponseProcessor<R> processor, boolean background) throws IOException {
        throw new AssertionError("do override");
    }

    @Override
    public <R> FutureResponse<R> send(int serviceId, ByteBuffer payload, ResponseProcessor<R> processor, boolean background) throws IOException {
        throw new AssertionError("do override");
    }

    @Override
    public FutureResponse<Void> updateCredential(Credential credential) throws IOException {
        throw new AssertionError("do override");
    }

    @Override
    public FutureResponse<Void> updateExpirationTime(long time, TimeUnit unit) throws IOException {
        throw new AssertionError("do override");
    }

    @Override
    public void connect(Wire sessionWire) {
        throw new AssertionError("do override");
    }

    @Override
    public Wire getWire() {
        throw new AssertionError("do override");
    }

    @Override
    public void put(ServerResource resource) {
        throw new AssertionError("do override");
    }

    @Override
    public void remove(ServerResource resource) {
        throw new AssertionError("do override");
    }
}
