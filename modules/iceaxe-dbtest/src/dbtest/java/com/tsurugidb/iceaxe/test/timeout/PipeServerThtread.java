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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.test.util.DbTestConnector;

class PipeServerThtread extends TimeoutServerThread {
    private static final Logger LOG = LoggerFactory.getLogger(PipeServerThtread.class);

    private volatile boolean write = true;

    public PipeServerThtread() {
        super("iceaxe-dbtest.pipeServer");
    }

    @Override
    protected void run(ServerSocket server) throws IOException, InterruptedException {
        try (var dbSocket = DbTestConnector.createSocket(); //
                var socket = server.accept()) {
            new PipeStreamThread("iceaxe-dbtest.client->db", "send", socket, dbSocket).start();
            new PipeStreamThread("iceaxe-dbtest.db->client", "recv", dbSocket, socket).start();

            setPriority(MIN_PRIORITY);
            while (!isStop()) {
                TimeUnit.MILLISECONDS.sleep(100);
            }
        }
    }

    public void setPipeWrite(boolean write) {
        if (this.write == write) {
            return;
        }

        try {
            // フラグを切り替えるまでに行われた通信が処理されるまで充分待つ
            TimeUnit.MILLISECONDS.sleep(300);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        this.write = write;
        LOG.trace("pipeWrite={}", write);
    }

    private class PipeStreamThread extends Thread {
        private final Logger log = LoggerFactory.getLogger(getClass());

        private final String role;
        private final InputStream is;
        private final OutputStream os;

        public PipeStreamThread(String name, String role, Socket inSocket, Socket outSocket) throws IOException {
            super(name);
            this.role = role;
            this.is = inSocket.getInputStream();
            this.os = outSocket.getOutputStream();
        }

        @Override
        public void run() {
            setPriority(MAX_PRIORITY);

            byte[] buffer = new byte[1024];
            try {
                for (;;) {
                    int len = is.read(buffer);
                    if (len < 0) {
                        break;
                    }

                    if (write) {
                        debugDump(false, buffer, len);
                        os.write(buffer, 0, len);
                    } else {
                        debugDump(true, buffer, len);
                    }
                }
//          } catch (SocketException e) {
//              LOG.trace(e.getMessage());
            } catch (Exception e) {
                log.trace(e.getMessage(), e);
            }
            log.trace("PipeStreamThread end");
        }

        private void debugDump(boolean ignore, byte[] buffer, int length) {
            if (!log.isDebugEnabled()) {
                return;
            }

            String message;
            if (ignore) {
                message = "ignore-" + role;
            } else {
                message = role;
            }

            var sb = new StringBuilder(3 * length);
            for (int i = 0; i < length; i++) {
                if (sb.length() != 0) {
                    sb.append(" ");
                }
                sb.append(String.format("%02x", buffer[i]));
            }
            log.debug("{} len={}, buffer={}", message, String.format("%3d", length), sb);
        }
    }
}
