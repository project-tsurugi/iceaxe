package com.tsurugidb.iceaxe.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;

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
    void testCommitType() {
        var info = new TgSessionInfo().commitType(TgCommitType.STORED);
        assertEquals(TgCommitType.STORED, info.commitType());
    }

    @Test
    void testOf() {
        var info = TgSessionInfo.of();
        assertNull(info.credential());
        var timeout = info.timeout(TgTimeoutKey.DEFAULT);
        assertEquals(Long.MAX_VALUE, timeout.value());
        assertEquals(TimeUnit.NANOSECONDS, timeout.unit());
        assertEquals(TgCommitType.DEFAULT, info.commitType());
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
        assertEquals(TgCommitType.DEFAULT, info.commitType());
    }

    @Test
    void testToString() {
        var empty = new TgSessionInfo();
        assertEquals("TgSessionInfo{credential=null, timeout={DEFAULT=9223372036854775807nanoseconds}, commitType=DEFAULT}", empty.toString());

        var info = TgSessionInfo.of("u1", "p1").timeout(TgTimeoutKey.DEFAULT, 123, TimeUnit.SECONDS).commitType(TgCommitType.STORED);
        assertEquals("TgSessionInfo{credential=UsernamePasswordCredential(name=u1), timeout={DEFAULT=123seconds}, commitType=STORED}", info.toString());
    }
}
