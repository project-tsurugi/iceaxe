package com.tsurugidb.iceaxe.example;

import java.net.URI;

import com.tsurugidb.iceaxe.TsurugiConnector;

/**
 * TsurugiConnector example
 *
 * @see Example02Session
 */
public class Example01Connector {

    static TsurugiConnector createConnector() {
        var connector = TsurugiConnector.createConnector("tcp://localhost:12345");
        return connector;
    }

    static TsurugiConnector createConnectorUri() {
        var endpoint = URI.create("tcp://localhost:12345");
        var connector = TsurugiConnector.createConnector(endpoint);
        return connector;
    }

    static TsurugiConnector createConnectorIpc() {
        String databaseName = "tateyama";
        var endpoint = "ipc:" + databaseName;
        var connector = TsurugiConnector.createConnector(endpoint);
        return connector;
    }
}
