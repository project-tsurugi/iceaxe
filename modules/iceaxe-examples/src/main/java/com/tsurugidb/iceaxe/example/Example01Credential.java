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
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.FileCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.RememberMeCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;

/**
 * Credential example
 */
public class Example01Credential {

    static void pass_credential_to_connector() throws IOException, InterruptedException {
        var endpoint = URI.create("tcp://localhost:12345");
        var credential = getCredential();
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.MINUTES);

        var connector = TsurugiConnector.of(endpoint, credential, sessionOption);
        try (var session = connector.createSession()) {
            // execute SQL
        }
    }

    static void pass_credential_to_createSession() throws IOException, InterruptedException {
        var endpoint = URI.create("tcp://localhost:12345");
        var credential = getCredential();
        var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.MINUTES);

        var connector = TsurugiConnector.of(endpoint);
        try (var session = connector.createSession(credential, sessionOption)) {
            // execute SQL
        }
    }

    public static Credential getCredential() {
//      return getNullCredential();
        return getUserPasswordCredential();
//      return getTokenCredential();
//      return getFileCredential();
    }

    static Credential getNullCredential() {
        return NullCredential.INSTANCE;
    }

    static Credential getUserPasswordCredential() {
        return new UsernamePasswordCredential("user", "password");
    }

    static Credential getTokenCredential() {
        return new RememberMeCredential("token");
    }

    static Credential getFileCredential() {
        var file = Path.of("/path/to/credential-file");
        try {
            return FileCredential.load(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
