package com.tsurugidb.iceaxe.transaction.manager;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.manager.event.TsurugiTmEventListener;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

class TsurugiTransactionManagerTest {

    @Test
    void findEventListener() throws Exception {
        class TestListener implements TsurugiTmEventListener {
        }

        try (var session = new TsurugiSession(null, TgSessionOption.of())) {
            {
                var tm = session.createTransactionManager();
                {
                    var opt = tm.findEventListener(l -> l instanceof TestListener);
                    assertTrue(opt.isEmpty());
                }
                {
                    var listener = new TestListener();
                    tm.addEventListener(listener);
                    var opt = tm.findEventListener(l -> l instanceof TestListener);
                    assertSame(listener, opt.get());
                }
            }
            {
                var setting = TgTmSetting.of(TgTxOption.ofOCC());
                var listener = new TestListener();
                setting.addEventListener(listener);
                var tm = session.createTransactionManager(setting);
                {
                    var opt = tm.findEventListener(l -> l instanceof TestListener);
                    assertSame(listener, opt.get());
                }

            }
        }
    }
}
