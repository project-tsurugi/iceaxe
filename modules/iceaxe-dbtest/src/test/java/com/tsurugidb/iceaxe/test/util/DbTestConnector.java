package com.tsurugidb.iceaxe.test.util;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;

public class DbTestConnector {

    private static final String SYSPROP_DBTEST_ENDPOINT = "tsurugi.dbtest.endpoint";

    private static URI staticEndpoint;

    private static URI getEndPoint() {
        if (staticEndpoint == null) {
            String endpoint = System.getProperty(SYSPROP_DBTEST_ENDPOINT, "tcp://localhost:12345");
            staticEndpoint = URI.create(endpoint);
        }
        return staticEndpoint;
    }

    public static URI assumeEndpointTcp() {
        URI endpoint = getEndPoint();
        String scheme = endpoint.getScheme();
        assumeTrue(scheme.equals("tcp"), "ednpoint is not tcp");
        return endpoint;
    }

    public static TsurugiConnector createConnector() {
        URI endpoint = getEndPoint();
        return TsurugiConnector.createConnector(endpoint);
    }

    public static TsurugiSession createSession() throws IOException {
        var info = TgSessionInfo.of();
        info.timeout(TgTimeoutKey.DEFAULT, 20, TimeUnit.SECONDS);

        var connector = createConnector();
        return connector.createSession(info);
    }

    public static Socket createSocket() {
        URI endpoint = assumeEndpointTcp();
        try {
            return new Socket(endpoint.getHost(), endpoint.getPort());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
