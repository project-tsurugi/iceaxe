package com.tsurugidb.iceaxe.test.util;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TgSessionShutdownType;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.session.event.TsurugiSessionEventListener;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.event.TsurugiTransactionEventListener;

public class DbTestConnector {
    private static final Logger LOG = LoggerFactory.getLogger(DbTestConnector.class);

    private static final String SYSPROP_DBTEST_ENDPOINT = "tsurugi.dbtest.endpoint";

    private static URI staticEndpoint;
    private static final List<TsurugiSession> staticSessionList = new CopyOnWriteArrayList<>();

    private static URI getEndPoint() {
        if (staticEndpoint == null) {
            String endpoint = System.getProperty(SYSPROP_DBTEST_ENDPOINT, "tcp://localhost:12345");
            staticEndpoint = URI.create(endpoint);
        }
        return staticEndpoint;
    }

    public static boolean isIpc() {
        URI endpoint = getEndPoint();
        String scheme = endpoint.getScheme();
        return scheme.equals("ipc");
    }

    public static boolean isTcp() {
        URI endpoint = getEndPoint();
        String scheme = endpoint.getScheme();
        return scheme.equals("tcp");
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

    private static final TgSessionShutdownType CLOSE_SHUTDOWN_TYPE = TgSessionShutdownType.GRACEFUL;

    public static TsurugiSession createSession() throws IOException {
        return createSession(20, TimeUnit.SECONDS);
    }

    public static TsurugiSession createSession(long time, TimeUnit unit) throws IOException {
        return createSession(time, unit, CLOSE_SHUTDOWN_TYPE);
    }

    public static TsurugiSession createSession(TgSessionShutdownType shutdownType) throws IOException {
        return createSession(20, TimeUnit.SECONDS, shutdownType);
    }

    public static TsurugiSession createSession(long time, TimeUnit unit, TgSessionShutdownType shutdownType) throws IOException {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, time, unit);
        sessionOption.setCloseShutdownType(shutdownType);

        var connector = createConnector();
        var session = connector.createSession(sessionOption);
        addSession(session);
//      session.addEventListener(SESSION_LISTENER);
        return session;
    }

    public static void addSession(TsurugiSession session) {
        staticSessionList.add(session);
        session.addEventListener(SESSION_CLOSE_LISTENER);
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

    private static final TsurugiSessionEventListener SESSION_CLOSE_LISTENER = new TsurugiSessionEventListener() {
        @Override
        public void closeSession(TsurugiSession session, long timeoutNanos, Throwable occurred) {
            staticSessionList.remove(session);
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

    public static void closeLeakSession() {
        int leak = 0;
        var list = List.copyOf(staticSessionList);
        for (var session : list) {
            if (!session.isClosed()) {
                LOG.error("session leak! {}", session);
                leak++;
                try {
                    session.close();
                } catch (Throwable e) {
                    LOG.warn("session close error", e);
                }
            }
        }

        if (leak > 0) {
            throw new AssertionError("session leak! " + leak);
        }
    }
}
