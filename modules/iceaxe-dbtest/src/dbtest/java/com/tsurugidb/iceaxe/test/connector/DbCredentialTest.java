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

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.tsubakuro.channel.common.connection.FileCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.RememberMeCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;

/**
 * credential test
 */
class DbCredentialTest extends DbTestTableTester {

    @Test
    void user() throws Exception {
        String user = DbTestConnector.getUser();
        assumeTrue(user != null);
        String password = DbTestConnector.getPassword();
        var credential = new UsernamePasswordCredential(user, password);

        var endpoint = DbTestConnector.getEndPoint();
        var connector = TsurugiConnector.of(endpoint, credential);
        try (var session = connector.createSession()) {
            session.getLowSession();
        }
    }

    @Test
    void authToken() throws Exception {
        String authToken = DbTestConnector.getAuthToken();
        assumeTrue(authToken != null);
        var credential = new RememberMeCredential(authToken);

        var endpoint = DbTestConnector.getEndPoint();
        var connector = TsurugiConnector.of(endpoint, credential);
        try (var session = connector.createSession()) {
            session.getLowSession();
        }
    }

    @Test
    void fileCredential() throws Exception {
        Path credentials = DbTestConnector.getCredentials();
        assumeTrue(credentials != null);
        var credential = FileCredential.load(credentials);

        var endpoint = DbTestConnector.getEndPoint();
        var connector = TsurugiConnector.of(endpoint, credential);
        try (var session = connector.createSession()) {
            session.getLowSession();
        }
    }
}
