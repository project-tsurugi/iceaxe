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
