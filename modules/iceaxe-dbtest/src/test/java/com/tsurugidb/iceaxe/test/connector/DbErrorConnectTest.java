package com.tsurugidb.iceaxe.test.connector;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.TgTxOption;

/**
 * connect error test
 */
public class DbErrorConnectTest {

    @Test
    void connectError() throws IOException, InterruptedException {
        try (var server = new ServerSocket(0)) {
            int port = server.getLocalPort();

            var thread = new ServerThtread(server);
            thread.start();

            var e = assertThrows(IOException.class, () -> connect(port));
            assertInstanceOf(TimeoutException.class, e.getCause());
        }
    }

    private void connect(int port) throws IOException {
        var endpoint = URI.create("tcp://localhost:" + port);
        var connector = TsurugiConnector.createConnector(endpoint);

        var info = TgSessionInfo.of();
        info.timeout(TgTimeoutKey.DEFAULT, 2, TimeUnit.SECONDS);
        try (var session = connector.createSession(info); //
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
