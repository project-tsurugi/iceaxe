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
package com.tsurugidb.iceaxe.test.util;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.net.URI;
import java.nio.file.Path;
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
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.FileCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.RememberMeCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;

public class DbTestConnector {
    private static final Logger LOG = LoggerFactory.getLogger(DbTestConnector.class);

    private static final String SYSPROP_DBTEST_ENDPOINT = "tsurugi.dbtest.endpoint";
    private static final String SYSPROP_DBTEST_USER = "tsurugi.dbtest.user";
    private static final String SYSPROP_DBTEST_PASSWORD = "tsurugi.dbtest.password";
    private static final String SYSPROP_DBTEST_AUTH_TOKEN = "tsurugi.dbtest.auth-token";
    private static final String SYSPROP_DBTEST_CREDENTIALS = "tsurugi.dbtest.credentials";

    private static URI staticEndpoint;
    private static Credential staticCredential;
    private static final List<TsurugiSession> staticSessionList = new CopyOnWriteArrayList<>();

    private static String sessionLabel;

    public static URI getEndPoint() {
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

    public static Credential getCredential() {
        if (staticCredential == null) {
            staticCredential = createCredential();
        }
        return staticCredential;
    }

    private static Credential createCredential() {
        String user = getUser();
        if (user != null) {
            String password = getPassword();
            return new UsernamePasswordCredential(user, password);
        }

        String authToken = getAuthToken();
        if (authToken != null) {
            return new RememberMeCredential(authToken);
        }

        Path credentials = getCredentials();
        if (credentials != null) {
            try {
                return FileCredential.load(credentials);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }

//      return NullCredential.INSTANCE;
        return new UsernamePasswordCredential("tsurugi", "password");
    }

    public static String getUser() {
        return getSystemProperty(SYSPROP_DBTEST_USER);
    }

    public static String getPassword() {
        return getSystemProperty(SYSPROP_DBTEST_PASSWORD);
    }

    public static String getAuthToken() {
        return getSystemProperty(SYSPROP_DBTEST_AUTH_TOKEN);
    }

    public static Path getCredentials() {
        String credentials = getSystemProperty(SYSPROP_DBTEST_CREDENTIALS);
        if (credentials == null) {
            return null;
        }
        return Path.of(credentials);
    }

    private static String getSystemProperty(String key) {
        String value = System.getProperty(key);
        if (value != null && value.isEmpty()) {
            return null;
        }
        return value;
    }

    public static void setSessionLabel(String label) {
        sessionLabel = label;
    }

    public static String getSessionLabel() {
        return sessionLabel;
    }

    public static TsurugiConnector createConnector() {
        var credential = getCredential();
        return createConnector(credential);
    }

    public static TsurugiConnector createConnector(Credential credential) {
        URI endpoint = getEndPoint();
        return TsurugiConnector.of(endpoint, credential).setApplicationName("iceaxe-dbtest");
    }

    private static final TgSessionShutdownType CLOSE_SHUTDOWN_TYPE = TgSessionShutdownType.GRACEFUL;

    public static TsurugiSession createSession() throws IOException {
        return createSession(sessionLabel);
    }

    public static TsurugiSession createSession(String label) throws IOException {
        return createSession(label, 20, TimeUnit.SECONDS, CLOSE_SHUTDOWN_TYPE);
    }

    public static TsurugiSession createSession(Credential credential, String label) throws IOException {
        var connector = createConnector(credential);
        return createSession(connector, label, 20, TimeUnit.SECONDS, CLOSE_SHUTDOWN_TYPE);
    }

    public static TsurugiSession createSession(long time, TimeUnit unit) throws IOException {
        return createSession(sessionLabel, time, unit, CLOSE_SHUTDOWN_TYPE);
    }

    public static TsurugiSession createSession(TgSessionShutdownType shutdownType) throws IOException {
        return createSession(sessionLabel, 20, TimeUnit.SECONDS, shutdownType);
    }

    public static TsurugiSession createSession(String label, long time, TimeUnit unit, TgSessionShutdownType shutdownType) throws IOException {
        var connector = createConnector();
        return createSession(connector, label, time, unit, shutdownType);
    }

    public static TsurugiSession createSession(TsurugiConnector connector, String label, long time, TimeUnit unit, TgSessionShutdownType shutdownType) throws IOException {
        var sessionOption = TgSessionOption.of();
        sessionOption.setLabel(label);
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, time, unit);
        sessionOption.setCloseShutdownType(shutdownType);

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
