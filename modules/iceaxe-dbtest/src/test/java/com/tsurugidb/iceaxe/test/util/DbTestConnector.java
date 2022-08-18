package com.tsurugidb.iceaxe.test.util;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;

public class DbTestConnector {

    public static TsurugiConnector createConnector() {
        var endpoint = URI.create("tcp://localhost:12345");
        return TsurugiConnector.createConnector(endpoint);
    }

    public static TsurugiSession createSession() throws IOException {
        var info = TgSessionInfo.of();
        info.timeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.MINUTES);

        var connector = createConnector();
        return connector.createSession(info);
    }
}
