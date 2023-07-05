package com.tsurugidb.iceaxe.test.util;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.session.event.TsurugiSessionEventListener;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.event.TsurugiTransactionEventListener;

public class DbTestConnector {
    private static final Logger LOG = LoggerFactory.getLogger(DbTestConnector.class);

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
        return TsurugiConnector.of(endpoint);
    }

    public static TsurugiSession createSession() throws IOException {
        return createSession(20, TimeUnit.SECONDS);
    }

    public static TsurugiSession createSession(long time, TimeUnit unit) throws IOException {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, time, unit);

        var connector = createConnector();
        var session = connector.createSession(sessionOption);
//      session.addEventListener(SESSION_LISTENER);
        return session;
    }

    @SuppressWarnings("unused")
    private static final TsurugiSessionEventListener SESSION_LISTENER = new TsurugiSessionEventListener() {
        private final TsurugiTransactionEventListener TRANSACTION_LISTENER = new TsurugiTransactionEventListener() {
            @Override
            public void lowTransactionGetEnd(TsurugiTransaction transaction, String transactionId, Throwable occurred) {
                LOG.info("transactionId={}, {}", transactionId, transaction.getTransactionOption());
            }
        };

        @Override
        public void createTransaction(TsurugiTransaction transaction) {
//          transaction.addEventListener(TRANSACTION_LISTENER);
            try {
                LOG.info("transactionId={}, {}", transaction.getTransactionId(), transaction.getTransactionOption());
            } catch (IOException | InterruptedException e) {
                LOG.info("getTransactionId error {}", transaction.getTransactionOption(), e);
            }
        }
    };

    public static Socket createSocket() {
        URI endpoint = assumeEndpointTcp();
        try {
            return new Socket(endpoint.getHost(), endpoint.getPort());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
