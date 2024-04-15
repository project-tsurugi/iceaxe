package com.tsurugidb.iceaxe.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.net.URI;
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

        @Override
        public SessionBuilder createLowSessionBuilder(Credential credential, TgSessionOption sessionOption) {
            return super.createLowSessionBuilder(credential, sessionOption);
        }
    }

    @Test
    void label() {
        var sessionOption = new TgSessionOption().setLabel("label-test");
        assertEquals("label-test", sessionOption.getLabel());

        var connector = new SessionOptionTestConnector();
        var lowBuilder = connector.createLowSessionBuilder(null, sessionOption);
        String actual = getField(lowBuilder, "connectionLabel");
        assertEquals("label-test", actual);
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

    // FIXME リフレクションを使わずにSessionBuilderから取得したい
    private static String getField(SessionBuilder lowBuilder, String name) {
        try {
            var field = lowBuilder.getClass().getDeclaredField(name);
            field.setAccessible(true);
            return (String) field.get(lowBuilder);
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
    void testOf() {
        var sessionOption = TgSessionOption.of();

        assertNull(sessionOption.getLabel());
        assertNull(sessionOption.getApplicationName());
        var timeout = sessionOption.getTimeout(TgTimeoutKey.DEFAULT);
        assertEquals(Long.MAX_VALUE, timeout.value());
        assertEquals(TimeUnit.NANOSECONDS, timeout.unit());
        assertEquals(TgCommitType.DEFAULT, sessionOption.getCommitType());
    }

    @Test
    void testToString() {
        var empty = new TgSessionOption();
        assertEquals("TgSessionOption{label=null, applicationName=null, timeout={DEFAULT=9223372036854775807nanoseconds}, commitType=DEFAULT}", empty.toString());

        var sessionOption = TgSessionOption.of() //
                .setLabel("test") //
                .setApplicationName("test-app") //
                .setTimeout(TgTimeoutKey.DEFAULT, 123, TimeUnit.SECONDS) //
                .setCommitType(TgCommitType.STORED);
        assertEquals("TgSessionOption{label=test, applicationName=test-app, timeout={DEFAULT=123seconds}, commitType=STORED}", sessionOption.toString());
    }
}
