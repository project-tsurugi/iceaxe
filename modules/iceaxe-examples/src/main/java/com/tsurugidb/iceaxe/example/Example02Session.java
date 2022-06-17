package com.tsurugidb.iceaxe.example;

import java.io.IOException;

import com.nautilus_technologies.tsubakuro.channel.common.connection.RememberMeCredential;
import com.nautilus_technologies.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;

/**
 * TsurugiSession example
 */
public class Example02Session {

    /**
     * @see Example01Connector
     */
    void main() throws IOException {
        var connector = TsurugiConnector.createConnector("tcp://localhost:12345");
        sessionUserPassword(connector);
        sessionCredentialUserPassword(connector);
        sessionCredentialToken(connector);
    }

    void sessionUserPassword(TsurugiConnector connector) throws IOException {
        var info = TgSessionInfo.of("user", "password");
        try (var session = connector.createSession(info)) {
//          session.createPreparedStatement()
//          session.createTransactionManager()
        }
    }

    void sessionCredentialUserPassword(TsurugiConnector connector) throws IOException {
        var credential = new UsernamePasswordCredential("user", "password");
        var info = TgSessionInfo.of(credential);
        try (var session = connector.createSession(info)) {
//          session.createPreparedStatement()
//          session.createTransactionManager()
        }
    }

    void sessionCredentialToken(TsurugiConnector connector) throws IOException {
        var credential = new RememberMeCredential("token");
        var info = TgSessionInfo.of(credential);
        try (var session = connector.createSession(info)) {
//          session.createPreparedStatement()
//          session.createTransactionManager()
        }
    }
}
