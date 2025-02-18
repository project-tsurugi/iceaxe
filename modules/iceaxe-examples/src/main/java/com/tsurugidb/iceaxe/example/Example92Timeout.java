/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * example to specify timeout
 */
public class Example92Timeout {

    void timeoutBySessionOption(TsurugiConnector connector) throws IOException, InterruptedException {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.MINUTES);
        sessionOption.setTimeout(TgTimeoutKey.TRANSACTION_COMMIT, 1, TimeUnit.HOURS);

        try (var session = connector.createSession(sessionOption)) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);
            tm.execute(transaction -> {
                // do sql
            });
        }
    }

    void timeoutByTmSetting(TsurugiConnector connector) throws IOException, InterruptedException {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.MINUTES);

        try (var session = connector.createSession(sessionOption)) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC()).commitTimeout(1, TimeUnit.HOURS);
            var tm = session.createTransactionManager(setting);

            tm.execute(transaction -> {
                // do sql
            });
        }
    }

    void timeoutByTransaction(TsurugiConnector connector) throws IOException, InterruptedException {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.MINUTES);

        try (var session = connector.createSession(sessionOption)) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);
            tm.execute(transaction -> {
                transaction.setCommitTimeout(1, TimeUnit.HOURS);

                // do sql
            });
        }
    }
}
