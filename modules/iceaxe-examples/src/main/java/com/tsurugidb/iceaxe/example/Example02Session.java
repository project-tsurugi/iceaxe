package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.nio.file.Path;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.tsubakuro.channel.common.connection.FileCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.RememberMeCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;

/**
 * TsurugiSession example
 */
public class Example02Session {

    void main() throws IOException {
        var connector = Example01Connector.createConnector();
        sessionNullCredential(connector);
        sessionUserPasswordCredential(connector);
        sessionTokenCredential(connector);
        sessionFileCredential(connector);
    }

    void sessionNullCredential(TsurugiConnector connector) throws IOException {
        var credential = NullCredential.INSTANCE;
        var info = TgSessionInfo.of(credential);
        try (var session = connector.createSession(info)) {
//          session.createPreparedStatement()
//          session.createTransactionManager()
        }
    }

    void sessionUserPasswordCredential(TsurugiConnector connector) throws IOException {
        var credential = new UsernamePasswordCredential("user", "password");
        var info = TgSessionInfo.of(credential);
        try (var session = connector.createSession(info)) {
//          session.createPreparedStatement()
//          session.createTransactionManager()
        }
    }

    void sessionTokenCredential(TsurugiConnector connector) throws IOException {
        var credential = new RememberMeCredential("token");
        var info = TgSessionInfo.of(credential);
        try (var session = connector.createSession(info)) {
//          session.createPreparedStatement()
//          session.createTransactionManager()
        }
    }

    void sessionFileCredential(TsurugiConnector connector) throws IOException {
        var file = Path.of("/path/to/credential.json");
        var credential = FileCredential.load(file);
        var info = TgSessionInfo.of(credential);
        try (var session = connector.createSession(info)) {
//          session.createPreparedStatement()
//          session.createTransactionManager()
        }
    }

    static TsurugiSession createSession() throws IOException {
        var connector = Example01Connector.createConnector();
        var info = TgSessionInfo.of(NullCredential.INSTANCE);
        return connector.createSession(info);
    }
}
