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
package com.tsurugidb.iceaxe.test.timeout;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;

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
        var credential = DbTestConnector.getCredential();
        var connector = TsurugiConnector.of(endpoint, credential);
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
