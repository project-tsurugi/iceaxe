package com.tsurugidb.iceaxe.test.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.tsubakuro.channel.stream.StreamLink;

/**
 * connect error test
 */
public class DbConnectErrorTest extends DbTestTableTester {

    @Test
    void connectError() throws Exception {
        try (var server = new ServerSocket(0)) {
            int port = server.getLocalPort();

            var thread = new ServerThtread(server);
            thread.start();

            var e = assertThrowsExactly(IOException.class, () -> connect(port));
            assertEqualsMessage("lost connection", e);
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

    @Test
    @Disabled // TODO remove Disabled
    void recvIllegalMessage() throws Exception {
        try (var server = new ServerSocket(0)) {
            int port = server.getLocalPort();

            var thread = new IllegalMessageServerThtread(server);
            thread.start();

            var e = assertThrowsExactly(IOException.class, () -> connect(port));
            assertEqualsMessage("lost connection", e);
        }
    }

    private static class IllegalMessageServerThtread extends Thread {

        private final ServerSocket server;

        public IllegalMessageServerThtread(ServerSocket server) {
            this.server = server;
        }

        @Override
        public void run() {
            try (var socket = server.accept()) {
                try (var os = socket.getOutputStream()) {
                    // 不正なメッセージを返す
                    var buf = new byte[128];
                    Arrays.fill(buf, (byte) 0xff);
                    buf[0] = StreamLink.RESPONSE_RESULT_SET_PAYLOAD;
                    os.write(buf);
                    os.flush();
                    TimeUnit.SECONDS.sleep(1);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private void connect(int port) throws Exception {
        var endpoint = URI.create("tcp://localhost:" + port);
        var connector = TsurugiConnector.of(endpoint);

        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 3, TimeUnit.SECONDS);
        try (var session = connector.createSession(sessionOption)) {
            session.getLowSession();
        }
    }

    @Test
    void sendIllegalMessage() throws Exception {
        try (var socket = DbTestConnector.createSocket()) {
            try (var os = socket.getOutputStream();) {
                // 不正なメッセージを送信する
                var buf = new byte[128];
                Arrays.fill(buf, (byte) 0xff);
                os.write(buf);
                os.flush();
                try (var is = socket.getInputStream()) {
                    try {
                        int len = is.read(buf);
                        assertEquals(-1, len);
                    } catch (SocketException e) {
                        assertEqualsMessage("Connection reset", e);
                    }
                }
            }
        }
    }
}
