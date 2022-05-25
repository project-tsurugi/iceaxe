package com.tsurugi.iceaxe.session;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugi.iceaxe.session.TgSessionInfo.TgTimeoutKey;

class TgSessionInfoTest {

    @Test
    void testUser() {
        var info = new TgSessionInfo().user("u1");
        assertEquals("u1", info.user());
    }

    @Test
    void testPassword() {
        var info = new TgSessionInfo().password("p1");
        assertEquals("p1", info.password());
    }

    @Test
    void testTimeout() {
        var info = new TgSessionInfo().timeout(TgTimeoutKey.DEFAULT, 123, TimeUnit.SECONDS);
        assertEquals(123L, info.timeout(TgTimeoutKey.DEFAULT).value());
        assertEquals(TimeUnit.SECONDS, info.timeout(TgTimeoutKey.DEFAULT).unit());
    }

    @Test
    void testOf() {
        var info = TgSessionInfo.of();
        assertNull(info.user());
        assertNull(info.password());
        assertEquals(Long.MAX_VALUE, info.timeout(TgTimeoutKey.DEFAULT).value());
        assertEquals(TimeUnit.NANOSECONDS, info.timeout(TgTimeoutKey.DEFAULT).unit());
    }

    @Test
    void testOfUser() {
        var info = TgSessionInfo.of("u1", "p1");
        assertEquals("u1", info.user());
        assertEquals("p1", info.password());
        assertEquals(Long.MAX_VALUE, info.timeout(TgTimeoutKey.DEFAULT).value());
        assertEquals(TimeUnit.NANOSECONDS, info.timeout(TgTimeoutKey.DEFAULT).unit());
    }

    @Test
    void testToString() {
        var empty = new TgSessionInfo();
        assertEquals("TgSessionInfo{user=null, password=null, timeout={DEFAULT=9223372036854775807nanoseconds}}", empty.toString());

        var info = TgSessionInfo.of("u1", "p1").timeout(TgTimeoutKey.DEFAULT, 123, TimeUnit.SECONDS);
        assertEquals("TgSessionInfo{user=u1, password=???, timeout={DEFAULT=123seconds}}", info.toString());
    }
}
