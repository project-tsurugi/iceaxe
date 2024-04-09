package com.tsurugidb.iceaxe.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.session.event.TsurugiSessionEventListener;

class TsurugiSessionTest {

    @Test
    void findEventListener() throws Exception {
        class TestListener implements TsurugiSessionEventListener {
        }

        var sessionOption = TgSessionOption.of();
        try (var session = new TsurugiSession(null, sessionOption)) {
            {
                var opt = session.findEventListener(l -> l instanceof TestListener);
                assertTrue(opt.isEmpty());
            }
            {
                var listener = new TestListener();
                session.addEventListener(listener);
                var opt = session.findEventListener(l -> l instanceof TestListener);
                assertSame(listener, opt.get());
            }
        }
    }

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
