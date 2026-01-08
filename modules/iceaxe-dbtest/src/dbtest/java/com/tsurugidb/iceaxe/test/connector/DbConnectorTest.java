/*
 * Copyright 2023-2026 Project Tsurugi.
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
package com.tsurugidb.iceaxe.test.connector;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * connector test
 */
class DbConnectorTest extends DbTestTableTester {

    @Test
    void label() throws Exception {
        var sessionOption = TgSessionOption.of().setLabel("test-label");

        var connector = DbTestConnector.createConnector();
        try (var session = connector.createSession(sessionOption)) {
            // TODO sessionからlabelを取得して確認したい
        }
    }

    @Test
    void applicationName() throws Exception {
        var sessionOption = TgSessionOption.of().setApplicationName("test-app");

        var connector = DbTestConnector.createConnector();
        try (var session = connector.createSession(sessionOption)) {
            // TODO sessionからapplicationNameを取得して確認したい
        }
    }

    @Test
    void doNothing() throws Exception {
        try (var socket = DbTestConnector.createSocket()) {
            // do nothing
        }

        // tsurugidbプロセスが落ちていなければ、正常に動作する
        var session = getSession();
        session.findTableMetadata(TEST);
    }
}
