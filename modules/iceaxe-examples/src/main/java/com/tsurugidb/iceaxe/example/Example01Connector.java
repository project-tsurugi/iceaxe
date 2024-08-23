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
