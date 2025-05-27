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

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.transaction.TgCommitOption;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * example to specify commitOption
 */
public class Example91CommitOption {

    void commitOption_by_sessionOption(TsurugiConnector connector) throws IOException, InterruptedException {
        var sessionOption = TgSessionOption.of();
        var commitOption = TgCommitOption.of(TgCommitType.STORED);
        sessionOption.setCommitOption(commitOption);

        try (var session = connector.createSession(sessionOption)) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);
            tm.execute(transaction -> {
                // do sql
            });
        }
    }

    void commitOption_by_createTm(TsurugiConnector connector) throws IOException, InterruptedException {
        try (var session = connector.createSession()) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC()).commitOptioon(TgCommitOption.of(TgCommitType.STORED));
            var tm = session.createTransactionManager(setting);

            tm.execute(transaction -> {
                // do sql
            });
        }
    }

    void commitOption_by_tmExecute(TsurugiConnector connector) throws IOException, InterruptedException {
        try (var session = connector.createSession()) {
            var tm = session.createTransactionManager();

            var setting = TgTmSetting.of(TgTxOption.ofOCC()).commitOptioon(TgCommitOption.of(TgCommitType.STORED));
            tm.execute(setting, transaction -> {
                // do sql
            });
        }
    }
}
