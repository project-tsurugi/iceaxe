package com.tsurugi.iceaxe.session;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

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
        var info = new TgSessionInfo().timeout(123, TimeUnit.SECONDS);
        assertEquals(123L, info.timeoutTime());
        assertEquals(TimeUnit.SECONDS, info.timeoutUnit());
    }

    @Test
    void testOf() {
        var info = TgSessionInfo.of();
        assertNull(info.user());
        assertNull(info.password());
        assertEquals(Long.MAX_VALUE, info.timeoutTime());
        assertEquals(TimeUnit.NANOSECONDS, info.timeoutUnit());
    }

    @Test
    void testOfUser() {
        var info = TgSessionInfo.of("u1", "p1");
        assertEquals("u1", info.user());
        assertEquals("p1", info.password());
        assertEquals(Long.MAX_VALUE, info.timeoutTime());
        assertEquals(TimeUnit.NANOSECONDS, info.timeoutUnit());
    }

    @Test
    void testToString() {
        var empty = new TgSessionInfo();
        assertEquals("TgSessionInfo{user=null, password=null, timeout=9223372036854775807NANOSECONDS}", empty.toString());

        var info = TgSessionInfo.of("u1", "p1").timeout(123, TimeUnit.SECONDS);
        assertEquals("TgSessionInfo{user=u1, password=???, timeout=123SECONDS}", info.toString());
    }
}
