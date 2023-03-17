package com.tsurugidb.iceaxe.test.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * connect error test
 */
public class DbConnectErrorTest {

    @Test
    void connectError() throws IOException {
        try (var server = new ServerSocket(0)) {
            int port = server.getLocalPort();

            var thread = new ServerThtread(server);
            thread.start();

            var e = assertThrowsExactly(IOException.class, () -> connect(port));
            try {
                assertEquals("Server crashed", e.getMessage());
            } catch (AssertionFailedError t) {
                throw e;
            }
        }
    }

    private void connect(int port) throws IOException {
        var endpoint = URI.create("tcp://localhost:" + port);
        var connector = TsurugiConnector.of(endpoint);

        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 3, TimeUnit.SECONDS);
        try (var session = connector.createSession(sessionOption); //
                var transaction = session.createTransaction(TgTxOption.ofOCC())) {
        }
    }

    private static class ServerThtread extends Thread {

        private final ServerSocket server;

        public ServerThtread(ServerSocket server) {
            this.server = server;
        }

        @Override
        public void run() {
            // クライアントから接続された後、何も返さずにクローズする
            try (var socket = server.accept()) {
                TimeUnit.SECONDS.sleep(1);
                // don't respond
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
