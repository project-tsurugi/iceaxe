package com.tsurugidb.iceaxe.test.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;

public class DbTestConnector {

    private static final String DB_HOST = "localhost";
    private static final int DB_PORT = 12345;

    public static TsurugiConnector createConnector() {
        var endpoint = URI.create("tcp://" + DB_HOST + ":" + DB_PORT);
        return TsurugiConnector.createConnector(endpoint);
    }

    public static TsurugiSession createSession() throws IOException {
        var info = TgSessionInfo.of();
        info.timeout(TgTimeoutKey.DEFAULT, 20, TimeUnit.SECONDS);

        var connector = createConnector();
        return connector.createSession(info);
    }

    public static Socket createSocket() {
        try {
            return new Socket(DB_HOST, DB_PORT);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
