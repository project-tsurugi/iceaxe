package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.net.URI;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionOption;
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
//          session.createStatement()
//          session.createTransactionManager()
        }
    }

    void createSession_option(URI endpoint, Credential credential) throws IOException, InterruptedException {
        var connector = TsurugiConnector.of(endpoint, credential);
        var sessionOption = TgSessionOption.of();
        try (var session = connector.createSession(sessionOption)) {
//          session.createStatement()
//          session.createTransactionManager()
        }
    }

    void createSession_credential(URI endpoint, Credential credential) throws IOException, InterruptedException {
        var connector = TsurugiConnector.of(endpoint);
        var sessionOption = TgSessionOption.of();
        try (var session = connector.createSession(credential, sessionOption)) {
//          session.createStatement()
//          session.createTransactionManager()
        }
    }

    static TsurugiSession createSession() throws IOException {
        var connector = Example01Connector.createConnector();
        return connector.createSession();
    }
}
