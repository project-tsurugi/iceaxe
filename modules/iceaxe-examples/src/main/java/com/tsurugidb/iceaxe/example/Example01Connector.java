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

import java.net.URI;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.TgCommitType;

/**
 * TsurugiConnector example
 *
 * @see Example02Session
 */
public class Example01Connector {

    static TsurugiConnector createConnector() {
        var endpoint = URI.create("tcp://localhost:12345");
        var credential = Example01Credential.getCredential();
        var connector = TsurugiConnector.of(endpoint, credential);
//      var connector = TsurugiConnector.of("application-name", endpoint, credential);
        return connector;
    }

    static TsurugiConnector createConnector_endpointString() {
        var endpoint = "tcp://localhost:12345";
        var credential = Example01Credential.getCredential();
        var connector = TsurugiConnector.of(endpoint, credential);
//      var connector = TsurugiConnector.of("application-name", endpoint, credential);
        return connector;
    }

    static TsurugiConnector createConnectorIpc() {
        String databaseName = "tateyama";
        var endpoint = "ipc:" + databaseName;
        var credential = Example01Credential.getCredential();
        var connector = TsurugiConnector.of(endpoint, credential);
        return connector;
    }

    static TsurugiConnector createConnectorWithSessionOption() {
        var endpoint = URI.create("tcp://localhost:12345");
        var credential = Example01Credential.getCredential();
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.MINUTES).setCommitType(TgCommitType.DEFAULT);
        var connector = TsurugiConnector.of(endpoint, credential, sessionOption);
//      var connector = TsurugiConnector.of("application-name", endpoint, credential, sessionOption);
        return connector;
    }
}
