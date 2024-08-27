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
package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionShutdownType;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;

/**
 * TsurugiSession example
 *
 * @see Example01Connector
 * @see Example02SessionOption
 */
public class Example02Session {

    void createSession(URI endpoint, Credential credential) throws IOException, InterruptedException {
        var sessionOption = TgSessionOption.of();
        var connector = TsurugiConnector.of(endpoint, credential, sessionOption);
        try (var session = connector.createSession()) {
//      try (var session = connector.createSession("session-label")) {
            // ...
        }
    }

    void createSession_option(URI endpoint, Credential credential) throws IOException, InterruptedException {
        var connector = TsurugiConnector.of(endpoint, credential);
        var sessionOption = TgSessionOption.of();
        try (var session = connector.createSession(sessionOption)) {
//      try (var session = connector.createSession("session-label", sessionOption)) {
            // ...
        }
    }

    void createSession_credential(URI endpoint, Credential credential) throws IOException, InterruptedException {
        var connector = TsurugiConnector.of(endpoint);
        var sessionOption = TgSessionOption.of();
        try (var session = connector.createSession(credential, sessionOption)) {
//      try (var session = connector.createSession("session-label", credential, sessionOption)) {
            // ...
        }
    }

    void shutdown(TsurugiConnector connector) throws IOException, InterruptedException {
        try (var session = connector.createSession()) {
            // ...
            session.shutdown(TgSessionShutdownType.GRACEFUL, 10, TimeUnit.SECONDS);
        }
    }

    void shutdown_option(TsurugiConnector connector) throws IOException, InterruptedException {
        var sessionOption = TgSessionOption.of();
        sessionOption.setCloseShutdownType(TgSessionShutdownType.GRACEFUL); // shutdown is called on close
        try (var session = connector.createSession(sessionOption)) {
            // ...
        }
    }

    static TsurugiSession createSession() throws IOException {
        var connector = Example01Connector.createConnector();
        return connector.createSession();
    }
}
