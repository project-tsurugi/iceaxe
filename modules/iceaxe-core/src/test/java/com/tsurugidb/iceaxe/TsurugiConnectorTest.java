/*
 * Copyright 2023-2024 Project Tsurugi.
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
package com.tsurugidb.iceaxe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.test.low.TestFutureResponse;
import com.tsurugidb.iceaxe.test.low.TestLowSession;
import com.tsurugidb.tsubakuro.channel.common.connection.Connector;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

class TsurugiConnectorTest {

    @Test
    void of_endpointStr() {
        String endpoint = "tcp://test:12345";
        var connector = TsurugiConnector.of(endpoint);
        assertNull(connector.getApplicationName());
        assertEquals(URI.create(endpoint), connector.getEndpoint());
        assertSame(NullCredential.INSTANCE, connector.getCredential());
        assertNotNull(connector.getSessionOption());
    }

    @Test
    void of_name_endpointStr() {
        String applicationName = "test-app";
        String endpoint = "tcp://test:12345";
        var connector = TsurugiConnector.of(applicationName, endpoint);
        assertEquals(applicationName, connector.getApplicationName());
        assertEquals(URI.create(endpoint), connector.getEndpoint());
        assertSame(NullCredential.INSTANCE, connector.getCredential());
        assertNotNull(connector.getSessionOption());
    }

    @Test
    void of_endpointUri() {
        URI endpoint = URI.create("tcp://test:12345");
        var connector = TsurugiConnector.of(endpoint);
        assertNull(connector.getApplicationName());
        assertEquals(endpoint, connector.getEndpoint());
        assertSame(NullCredential.INSTANCE, connector.getCredential());
        assertNotNull(connector.getSessionOption());
    }

    @Test
    void of_name_endpointUri() {
        String applicationName = "test-app";
        URI endpoint = URI.create("tcp://test:12345");
        var connector = TsurugiConnector.of(applicationName, endpoint);
        assertEquals(applicationName, connector.getApplicationName());
        assertEquals(endpoint, connector.getEndpoint());
        assertSame(NullCredential.INSTANCE, connector.getCredential());
        assertNotNull(connector.getSessionOption());
    }

    @Test
    void of_endpointStr_credential() {
        String endpoint = "tcp://test:12345";
        var credential = new UsernamePasswordCredential("test-user", "test-password");
        var connector = TsurugiConnector.of(endpoint, credential);
        assertNull(connector.getApplicationName());
        assertEquals(URI.create(endpoint), connector.getEndpoint());
        assertSame(credential, connector.getCredential());
        assertNotNull(connector.getSessionOption());
    }

    @Test
    void of_name_endpointStr_credential() {
        String applicationName = "test-app";
        String endpoint = "tcp://test:12345";
        var credential = new UsernamePasswordCredential("test-user", "test-password");
        var connector = TsurugiConnector.of(applicationName, endpoint, credential);
        assertEquals(applicationName, connector.getApplicationName());
        assertEquals(URI.create(endpoint), connector.getEndpoint());
        assertSame(credential, connector.getCredential());
        assertNotNull(connector.getSessionOption());
    }

    @Test
    void of_endpointUri_credential() {
        URI endpoint = URI.create("tcp://test:12345");
        var credential = new UsernamePasswordCredential("test-user", "test-password");
        var connector = TsurugiConnector.of(endpoint, credential);
        assertNull(connector.getApplicationName());
        assertEquals(endpoint, connector.getEndpoint());
        assertSame(credential, connector.getCredential());
        assertNotNull(connector.getSessionOption());
    }

    @Test
    void of_name_endpointUri_credential() {
        String applicationName = "test-app";
        URI endpoint = URI.create("tcp://test:12345");
        var credential = new UsernamePasswordCredential("test-user", "test-password");
        var connector = TsurugiConnector.of(applicationName, endpoint, credential);
        assertEquals(applicationName, connector.getApplicationName());
        assertEquals(endpoint, connector.getEndpoint());
        assertSame(credential, connector.getCredential());
        assertNotNull(connector.getSessionOption());
    }

    @Test
    void of_endpointStr_credential_option() {
        String endpoint = "tcp://test:12345";
        var credential = new UsernamePasswordCredential("test-user", "test-password");
        var sessionOption = TgSessionOption.of();
        var connector = TsurugiConnector.of(endpoint, credential, sessionOption);
        assertNull(connector.getApplicationName());
        assertEquals(URI.create(endpoint), connector.getEndpoint());
        assertSame(credential, connector.getCredential());
        assertSame(sessionOption, connector.getSessionOption());
    }

    @Test
    void of_name_endpointStr_credential_option() {
        String applicationName = "test-app";
        String endpoint = "tcp://test:12345";
        var credential = new UsernamePasswordCredential("test-user", "test-password");
        var sessionOption = TgSessionOption.of();
        var connector = TsurugiConnector.of(applicationName, endpoint, credential, sessionOption);
        assertEquals(applicationName, connector.getApplicationName());
        assertEquals(URI.create(endpoint), connector.getEndpoint());
        assertSame(credential, connector.getCredential());
        assertSame(sessionOption, connector.getSessionOption());
    }

    @Test
    void of_endpointUri_credential_option() {
        URI endpoint = URI.create("tcp://test:12345");
        var credential = new UsernamePasswordCredential("test-user", "test-password");
        var sessionOption = TgSessionOption.of();
        var connector = TsurugiConnector.of(endpoint, credential, sessionOption);
        assertNull(connector.getApplicationName());
        assertEquals(endpoint, connector.getEndpoint());
        assertSame(credential, connector.getCredential());
        assertSame(sessionOption, connector.getSessionOption());
    }

    @Test
    void of_name_endpointUri_credential_option() {
        String applicationName = "test-app";
        URI endpoint = URI.create("tcp://test:12345");
        var credential = new UsernamePasswordCredential("test-user", "test-password");
        var sessionOption = TgSessionOption.of();
        var connector = TsurugiConnector.of(applicationName, endpoint, credential, sessionOption);
        assertEquals(applicationName, connector.getApplicationName());
        assertEquals(endpoint, connector.getEndpoint());
        assertSame(credential, connector.getCredential());
        assertSame(sessionOption, connector.getSessionOption());
    }

    @Test
    void getEndpoint() {
        var endpoint = URI.create("ipc:test");
        var connector = new TsurugiConnector(null, endpoint, null, null);
        assertEquals(endpoint, connector.getEndpoint());
    }

    @Test
    void getSessionOption() {
        var sessionOption = TgSessionOption.of();
        var connector = new TsurugiConnector(null, null, null, sessionOption);
        assertSame(sessionOption, connector.getSessionOption());
    }

    @Test
    void getApplicationName() {
        {
            var connector = new TsurugiConnector(null, null, null, null);
            assertNull(connector.getApplicationName());
        }
        {
            var connector = new TsurugiConnector(null, null, null, null);
            connector.setApplicationName("test");
            assertEquals("test", connector.getApplicationName());
        }
    }

    @Test
    void findApplicationName() {
        {
            var connector = new TsurugiConnector(null, null, null, null);
            var option = TgSessionOption.of();
            assertEquals(Optional.empty(), connector.findApplicationName(option));
        }
        {
            var connector = new TsurugiConnector(null, null, null, null);
            connector.setApplicationName("test");
            var option = TgSessionOption.of();
            assertEquals(Optional.of("test"), connector.findApplicationName(option));
        }
        {
            var connector = new TsurugiConnector(null, null, null, null);
            var option = TgSessionOption.of().setApplicationName("test");
            assertEquals(Optional.of("test"), connector.findApplicationName(option));
        }
        {
            var connector = new TsurugiConnector(null, null, null, null);
            connector.setApplicationName("fail");
            var option = TgSessionOption.of().setApplicationName("test");
            assertEquals(Optional.of("test"), connector.findApplicationName(option));
        }
    }

    @Test
    void findEventListener() throws Exception {
        class TestListener implements Consumer<TsurugiSession> {
            @Override
            public void accept(TsurugiSession t) {
                // do nothing
            }
        }

        var connector = new TsurugiConnector(null, null, null, null);
        {
            var opt = connector.findEventListener(l -> l instanceof TestListener);
            assertTrue(opt.isEmpty());
        }
        {
            var listener = new TestListener();
            connector.addEventListener(listener);
            var opt = connector.findEventListener(l -> l instanceof TestListener);
            assertSame(listener, opt.get());
        }
    }

    @Test
    void createSession_void() throws Exception {
        var connector = TestConnector.create();
        try (var session = connector.createSession()) {
            var lowSession = (SessionBuilderTestLowSession) session.getLowSession();
            assertSame(DEFAULT_CREDENTIAL, lowSession.getCredential());
            assertEquals(DEFAULT_APPLICATION_NAME, lowSession.getApplicationName());
            assertEquals(DEFAULT_SESSION_LABEL, lowSession.getSessionLabel());
            assertEquals(DEFAULT_KEEP_ALIVE, lowSession.getKeepAlive());
        }
    }

    @Test
    void createSession_label() throws Exception {
        var connector = TestConnector.create();
        String label = "test-label";
        try (var session = connector.createSession(label)) {
            var lowSession = (SessionBuilderTestLowSession) session.getLowSession();
            assertSame(DEFAULT_CREDENTIAL, lowSession.getCredential());
            assertEquals(DEFAULT_APPLICATION_NAME, lowSession.getApplicationName());
            assertEquals(label, lowSession.getSessionLabel());
            assertEquals(DEFAULT_KEEP_ALIVE, lowSession.getKeepAlive());
        }
    }

    @Test
    void createSession_credential() throws Exception {
        var connector = TestConnector.create();
        var credential = new UsernamePasswordCredential("test-user", "test-password");
        try (var session = connector.createSession(credential)) {
            var lowSession = (SessionBuilderTestLowSession) session.getLowSession();
            assertSame(credential, lowSession.getCredential());
            assertEquals(DEFAULT_APPLICATION_NAME, lowSession.getApplicationName());
            assertEquals(DEFAULT_SESSION_LABEL, lowSession.getSessionLabel());
            assertEquals(DEFAULT_KEEP_ALIVE, lowSession.getKeepAlive());
        }
    }

    @Test
    void createSession_label_credential() throws Exception {
        var connector = TestConnector.create();
        String label = "test-label";
        var credential = new UsernamePasswordCredential("test-user", "test-password");
        try (var session = connector.createSession(label, credential)) {
            var lowSession = (SessionBuilderTestLowSession) session.getLowSession();
            assertSame(credential, lowSession.getCredential());
            assertEquals(DEFAULT_APPLICATION_NAME, lowSession.getApplicationName());
            assertEquals(label, lowSession.getSessionLabel());
            assertEquals(DEFAULT_KEEP_ALIVE, lowSession.getKeepAlive());
        }
    }

    @Test
    void createSession_option() throws Exception {
        var connector = TestConnector.create();
        var sessionOption = TgSessionOption.of().setApplicationName("option-app").setLabel("option-label").setKeepAlive(false);
        try (var session = connector.createSession(sessionOption)) {
            var lowSession = (SessionBuilderTestLowSession) session.getLowSession();
            assertSame(DEFAULT_CREDENTIAL, lowSession.getCredential());
            assertEquals("option-app", lowSession.getApplicationName());
            assertEquals("option-label", lowSession.getSessionLabel());
            assertFalse(lowSession.getKeepAlive());
        }
    }

    @Test
    void createSession_label_option() throws Exception {
        var connector = TestConnector.create();
        String label = "test-label";
        var sessionOption = TgSessionOption.of().setApplicationName("option-app").setLabel("option-label").setKeepAlive(false);
        try (var session = connector.createSession(label, sessionOption)) {
            var lowSession = (SessionBuilderTestLowSession) session.getLowSession();
            assertSame(DEFAULT_CREDENTIAL, lowSession.getCredential());
            assertEquals("option-app", lowSession.getApplicationName());
            assertEquals(label, lowSession.getSessionLabel());
            assertFalse(lowSession.getKeepAlive());
        }
    }

    @Test
    void createSession_credential_option() throws Exception {
        var connector = TestConnector.create();
        var credential = new UsernamePasswordCredential("test-user", "test-password");
        var sessionOption = TgSessionOption.of().setApplicationName("option-app").setLabel("option-label").setKeepAlive(false);
        try (var session = connector.createSession(credential, sessionOption)) {
            var lowSession = (SessionBuilderTestLowSession) session.getLowSession();
            assertSame(credential, lowSession.getCredential());
            assertEquals("option-app", lowSession.getApplicationName());
            assertEquals("option-label", lowSession.getSessionLabel());
            assertFalse(lowSession.getKeepAlive());
        }
    }

    @Test
    void createSession_label_credential_option() throws Exception {
        var connector = TestConnector.create();
        String label = "test-label";
        var credential = new UsernamePasswordCredential("test-user", "test-password");
        var sessionOption = TgSessionOption.of().setApplicationName("option-app").setLabel("option-label").setKeepAlive(false);
        try (var session = connector.createSession(label, credential, sessionOption)) {
            var lowSession = (SessionBuilderTestLowSession) session.getLowSession();
            assertSame(credential, lowSession.getCredential());
            assertEquals("option-app", lowSession.getApplicationName());
            assertEquals(label, lowSession.getSessionLabel());
            assertFalse(lowSession.getKeepAlive());
        }
    }

    static class SessionBuilderTestLowSession extends TestLowSession {
        private final SessionBuilder lowSessionBuilder;

        public SessionBuilderTestLowSession(SessionBuilder lowSessionBuilder) {
            this.lowSessionBuilder = lowSessionBuilder;
        }

        public Credential getCredential() {
            return getField("connectionCredential");
        }

        public String getSessionLabel() {
            return getField("connectionLabel");
        }

        public String getApplicationName() {
            return getField("applicationName");
        }

        public boolean getKeepAlive() {
            return getField("doKeepAlive");
        }

        @SuppressWarnings("unchecked")
        private <T> T getField(String name) {
            try {
                var field = lowSessionBuilder.getClass().getDeclaredField(name);
                field.setAccessible(true);
                return (T) field.get(lowSessionBuilder);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final String DEFAULT_APPLICATION_NAME = "default-app";
    private static final Credential DEFAULT_CREDENTIAL = new UsernamePasswordCredential("default-user", "default-password");
    private static final String DEFAULT_SESSION_LABEL = "default-label";
    private static final boolean DEFAULT_KEEP_ALIVE;
    static {
        try {
            var lowBuilder = SessionBuilder.connect("tcp://test:12345");
            var field = lowBuilder.getClass().getDeclaredField("doKeepAlive");
            field.setAccessible(true);
            DEFAULT_KEEP_ALIVE = field.getBoolean(lowBuilder);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static class TestConnector extends TsurugiConnector {

        public static TestConnector create() {
            var endpoint = URI.create("tcp://test:12345");
            var lowConnector = Connector.create(endpoint);
            var sessionOption = TgSessionOption.of().setLabel(DEFAULT_SESSION_LABEL).setApplicationName(DEFAULT_APPLICATION_NAME);
            return new TestConnector(lowConnector, endpoint, DEFAULT_CREDENTIAL, sessionOption);
        }

        protected TestConnector(Connector lowConnector, URI endpoint, Credential defaultCredential, TgSessionOption defaultSessionOption) {
            super(lowConnector, endpoint, defaultCredential, defaultSessionOption);
        }

        @Override
        protected FutureResponse<? extends Session> createLowSession(String label, Credential credential, TgSessionOption sessionOption) throws IOException {
            var lowBuilder = createLowSessionBuilder(label, credential, sessionOption);
            return new TestFutureResponse<>() {

                @Override
                protected Session getInternal() throws IOException, ServerException, InterruptedException {
                    return new SessionBuilderTestLowSession(lowBuilder);
                }
            };
        }
    }
}
