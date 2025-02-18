/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.iceaxe.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.tsubakuro.channel.common.connection.Connector;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.common.SessionBuilder;

class TgSessionOptionTest {

    private static class SessionOptionTestConnector extends TsurugiConnector {

        protected SessionOptionTestConnector() {
            super(Connector.create("tcp://test:12345"), URI.create("tcp://test:12345"), null, null);
        }

        public SessionBuilder createLowSessionBuilder(Credential credential, TgSessionOption sessionOption) {
            String label = null;
            return createLowSessionBuilder(label, credential, sessionOption);
        }

        @Override
        public SessionBuilder createLowSessionBuilder(String label, Credential credential, TgSessionOption sessionOption) {
            return super.createLowSessionBuilder(label, credential, sessionOption);
        }
    }

    @Test
    void label() {
        var sessionOption = new TgSessionOption().setLabel("label-test");
        assertEquals("label-test", sessionOption.getLabel());

        var connector = new SessionOptionTestConnector();
        var lowBuilder = connector.createLowSessionBuilder(null, sessionOption);
        String actual = getField(lowBuilder, "connectionLabel");
        assertNull(actual);
    }

    @Test
    void applicationName() {
        var sessionOption = new TgSessionOption().setApplicationName("test-app");
        assertEquals("test-app", sessionOption.getApplicationName());

        var connector = new SessionOptionTestConnector();
        var lowBuilder = connector.createLowSessionBuilder(null, sessionOption);
        String actual = getField(lowBuilder, "applicationName");
        assertEquals("test-app", actual);
    }

    @Test
    void keepAlive() {
        {
            var sessionOption = new TgSessionOption();
            testKeepAlive(Optional.empty(), sessionOption);
        }
        {
            var sessionOption = new TgSessionOption().setKeepAlive(true);
            testKeepAlive(Optional.of(true), sessionOption);
        }
        {
            var sessionOption = new TgSessionOption().setKeepAlive(false);
            testKeepAlive(Optional.of(false), sessionOption);
        }
        {
            var sessionOption = new TgSessionOption().setKeepAlive(null);
            testKeepAlive(Optional.empty(), sessionOption);
        }
    }

    private void testKeepAlive(Optional<Boolean> expected, TgSessionOption sessionOption) {
        assertEquals(expected, sessionOption.findKeepAlive());

        var connector = new SessionOptionTestConnector();
        var lowBuilder = connector.createLowSessionBuilder(null, sessionOption);
        boolean actual = getField(lowBuilder, "doKeepAlive");
        assertEquals(expected.orElseGet(() -> {
            var defaultBuilder = SessionBuilder.connect("tcp://test:12345");
            return getField(defaultBuilder, "doKeepAlive");
        }), actual);
    }

    // FIXME リフレクションを使わずにSessionBuilderから取得したい
    @SuppressWarnings("unchecked")
    private static <T> T getField(SessionBuilder lowBuilder, String name) {
        try {
            var field = lowBuilder.getClass().getDeclaredField(name);
            field.setAccessible(true);
            return (T) field.get(lowBuilder);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void timeout() {
        var sessionOption = new TgSessionOption().setTimeout(TgTimeoutKey.DEFAULT, 123, TimeUnit.SECONDS);
        var timeout = sessionOption.getTimeout(TgTimeoutKey.DEFAULT);
        assertEquals(123L, timeout.value());
        assertEquals(TimeUnit.SECONDS, timeout.unit());
    }

    @Test
    void commitType() {
        var sessionOption = new TgSessionOption().setCommitType(TgCommitType.STORED);
        assertEquals(TgCommitType.STORED, sessionOption.getCommitType());
    }

    @Test
    void closeShutdownType() {
        var sessionOption = new TgSessionOption().setCloseShutdownType(TgSessionShutdownType.GRACEFUL);
        assertEquals(TgSessionShutdownType.GRACEFUL, sessionOption.getCloseShutdownType());
    }

    @Test
    void testOf() {
        var sessionOption = TgSessionOption.of();

        assertNull(sessionOption.getLabel());
        assertNull(sessionOption.getApplicationName());
        var timeout = sessionOption.getTimeout(TgTimeoutKey.DEFAULT);
        assertEquals(Long.MAX_VALUE, timeout.value());
        assertEquals(TimeUnit.NANOSECONDS, timeout.unit());
        assertEquals(TgCommitType.DEFAULT, sessionOption.getCommitType());
        assertEquals(TgSessionShutdownType.NOTHING, sessionOption.getCloseShutdownType());
    }

    @Test
    void testToString() {
        var empty = new TgSessionOption();
        assertEquals("TgSessionOption{" //
                + "label=null" //
                + ", applicationName=null" //
                + ", timeout={DEFAULT=9223372036854775807nanoseconds}" //
                + ", commitType=DEFAULT" //
                + ", closeShutdownType=NOTHING" //
                + "}", empty.toString());

        var sessionOption = TgSessionOption.of() //
                .setLabel("test") //
                .setApplicationName("test-app") //
                .setTimeout(TgTimeoutKey.DEFAULT, 123, TimeUnit.SECONDS) //
                .setCommitType(TgCommitType.STORED) //
                .setCloseShutdownType(TgSessionShutdownType.GRACEFUL);
        assertEquals("TgSessionOption{" //
                + "label=test" //
                + ", applicationName=test-app" //
                + ", timeout={DEFAULT=123seconds}" //
                + ", commitType=STORED" //
                + ", closeShutdownType=GRACEFUL" //
                + "}", sessionOption.toString());
    }
}
