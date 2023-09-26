package com.tsurugidb.iceaxe.test.timeout;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;

import com.tsurugidb.iceaxe.TsurugiConnector;

abstract class TimeoutServerThread extends Thread implements Closeable {

    private final ServerSocket server;
    private volatile boolean stop = false;

    public TimeoutServerThread(String name) {
        super(name);
        try {
            this.server = new ServerSocket(0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public TsurugiConnector getTsurugiConnector() {
        int port = server.getLocalPort();
        var endpoint = URI.create("tcp://localhost:" + port);
        var connector = TsurugiConnector.of(endpoint);
        return connector;
    }

    @Override
    public void run() {
        try {
            run(server);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected abstract void run(ServerSocket server) throws IOException, InterruptedException;

    protected final boolean isStop() {
        return this.stop;
    }

    @Override
    public void close() throws IOException {
        this.stop = true;
        server.close();
    }
}
