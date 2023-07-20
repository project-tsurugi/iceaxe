package com.tsurugidb.iceaxe.session;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.TgCommitType;

class TgSessionOptionTest {

    @Test
    void testTimeout() {
        var sessionOption = new TgSessionOption().setTimeout(TgTimeoutKey.DEFAULT, 123, TimeUnit.SECONDS);
        var timeout = sessionOption.getTimeout(TgTimeoutKey.DEFAULT);
        assertEquals(123L, timeout.value());
        assertEquals(TimeUnit.SECONDS, timeout.unit());
    }

    @Test
    void testCommitType() {
        var sessionOption = new TgSessionOption().setCommitType(TgCommitType.STORED);
        assertEquals(TgCommitType.STORED, sessionOption.getCommitType());
    }

    @Test
    void testOf() {
        var sessionOption = TgSessionOption.of();
        var timeout = sessionOption.getTimeout(TgTimeoutKey.DEFAULT);
        assertEquals(Long.MAX_VALUE, timeout.value());
        assertEquals(TimeUnit.NANOSECONDS, timeout.unit());
        assertEquals(TgCommitType.DEFAULT, sessionOption.getCommitType());
    }

    @Test
    void testToString() {
        var empty = new TgSessionOption();
        assertEquals("TgSessionOption{label=null, timeout={DEFAULT=9223372036854775807nanoseconds}, commitType=DEFAULT}", empty.toString());

        var sessionOption = TgSessionOption.of().setLabel("test").setTimeout(TgTimeoutKey.DEFAULT, 123, TimeUnit.SECONDS).setCommitType(TgCommitType.STORED);
        assertEquals("TgSessionOption{label=test, timeout={DEFAULT=123seconds}, commitType=STORED}", sessionOption.toString());
    }
}
