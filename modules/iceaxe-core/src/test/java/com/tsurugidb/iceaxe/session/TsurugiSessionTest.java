package com.tsurugidb.iceaxe.session;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class TsurugiSessionTest {

    @Test
    void testToString() throws Exception {
        {
            var sessionOption = TgSessionOption.of();
            try (var session = new TsurugiSession(null, sessionOption)) {
                String hashCode = Integer.toHexString(session.hashCode());
                assertEquals("TsurugiSession@" + hashCode, session.toString());
            }
        }
        {
            var sessionOption = TgSessionOption.of().setLabel("test");
            try (var session = new TsurugiSession(null, sessionOption)) {
                assertEquals("TsurugiSession(test)", session.toString());
            }
        }
    }
}
