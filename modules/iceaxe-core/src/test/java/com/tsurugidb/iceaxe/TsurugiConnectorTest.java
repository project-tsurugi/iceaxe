package com.tsurugidb.iceaxe;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.session.TsurugiSession;

class TsurugiConnectorTest {

    @Test
    void findEventListener() throws Exception {
        class TestListener implements Consumer<TsurugiSession> {
            @Override
            public void accept(TsurugiSession t) {
                // do nothing
            }
        }

        var connector = new TsurugiConnector(null, null, null, null) {
        };
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
}
