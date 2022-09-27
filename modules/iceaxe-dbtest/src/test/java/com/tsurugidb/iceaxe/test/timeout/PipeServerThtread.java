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

    private volatile boolean send = true;

    public PipeServerThtread() {
        super("pipeServer");
    }

    @Override
    protected void run(ServerSocket server) throws IOException, InterruptedException {
        try (var dbSocket = DbTestConnector.createSocket(); //
                var socket = server.accept()) {
            new PipeStreamThread("db->client", dbSocket, socket).start();
            new PipeStreamThread("client->db", socket, dbSocket).start();
            for (;;) {
                if (isStop()) {
                    break;
                }

                TimeUnit.MILLISECONDS.sleep(50);
            }
        }
    }

    public void setSend(boolean send) {
        this.send = send;
    }

    private class PipeStreamThread extends Thread {

        private final InputStream is;
        private final OutputStream os;

        public PipeStreamThread(String name, Socket inSocket, Socket outSocket) throws IOException {
            super(name);
            this.is = inSocket.getInputStream();
            this.os = outSocket.getOutputStream();
        }

        @Override
        public void run() {
            byte[] buffer = new byte[1024];
            try {
                for (;;) {
                    int len = is.read(buffer);
                    if (len < 0) {
                        break;
                    }
                    if (send) {
                        os.write(buffer, 0, len);
                    }
                }
//          } catch (SocketException e) {
//              LOG.trace(e.getMessage());
            } catch (Exception e) {
                Logger log = LoggerFactory.getLogger(getClass());
                log.trace(e.getMessage(), e);
            }
        }
    }
}
