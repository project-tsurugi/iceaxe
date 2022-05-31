package com.tsurugidb.iceaxe.session;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.channel.common.connection.NullCredential;
import com.nautilus_technologies.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;

class TgSessionInfoTest {

    @Test
    void testCredential() {
        var info = new TgSessionInfo().credential(NullCredential.INSTANCE);
        assertEquals(NullCredential.INSTANCE, info.credential());
    }

    @Test
    void testTimeout() {
        var info = new TgSessionInfo().timeout(TgTimeoutKey.DEFAULT, 123, TimeUnit.SECONDS);
        var timeout = info.timeout(TgTimeoutKey.DEFAULT);
        assertEquals(123L, timeout.value());
        assertEquals(TimeUnit.SECONDS, timeout.unit());
    }

    @Test
    void testOf() {
        var info = TgSessionInfo.of();
        assertNull(info.credential());
        var timeout = info.timeout(TgTimeoutKey.DEFAULT);
        assertEquals(Long.MAX_VALUE, timeout.value());
        assertEquals(TimeUnit.NANOSECONDS, timeout.unit());
    }

    @Test
    void testOfUser() {
        var info = TgSessionInfo.of("u1", "p1");
        var credential = (UsernamePasswordCredential) info.credential();
        assertEquals("u1", credential.getName());
        assertEquals("p1", credential.getPassword().get());
        var timeout = info.timeout(TgTimeoutKey.DEFAULT);
        assertEquals(Long.MAX_VALUE, timeout.value());
        assertEquals(TimeUnit.NANOSECONDS, timeout.unit());
    }

    @Test
    void testToString() {
        var empty = new TgSessionInfo();
        assertEquals("TgSessionInfo{credential=null, timeout={DEFAULT=9223372036854775807nanoseconds}}", empty.toString());

        var info = TgSessionInfo.of("u1", "p1").timeout(TgTimeoutKey.DEFAULT, 123, TimeUnit.SECONDS);
        assertEquals("TgSessionInfo{credential=UsernamePasswordCredential(name=u1), timeout={DEFAULT=123seconds}}", info.toString());
    }
}
