/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
