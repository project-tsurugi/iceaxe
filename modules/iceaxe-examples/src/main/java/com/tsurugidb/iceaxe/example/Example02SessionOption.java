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

import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TgSessionShutdownType;
import com.tsurugidb.iceaxe.transaction.TgCommitOption;
import com.tsurugidb.iceaxe.transaction.TgCommitType;

/**
 * TgSessionOption example
 *
 * @see Example02Session
 */
public class Example02SessionOption {

    void label() {
        var sessionOption = TgSessionOption.of();
        sessionOption.setApplicationName("application name");
        sessionOption.setLabel("session label");

        // $TSURUGI_HOME/bin/tgctl session list --verbose
    }

    void keepAlive() {
        var sessionOption = TgSessionOption.of();
        sessionOption.setKeepAlive(true);
    }

    void timeout() {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.MINUTES);
        sessionOption.setTimeout(TgTimeoutKey.TRANSACTION_COMMIT, 1, TimeUnit.HOURS);
    }

    void commitOption() {
        var sessionOption = TgSessionOption.of();

        var commitOption = TgCommitOption.of(TgCommitType.DEFAULT);
        sessionOption.setCommitOption(commitOption);
    }

    void shutdownType() {
        var sessionOption = TgSessionOption.of();
        sessionOption.setCloseShutdownType(TgSessionShutdownType.GRACEFUL);
    }
}
