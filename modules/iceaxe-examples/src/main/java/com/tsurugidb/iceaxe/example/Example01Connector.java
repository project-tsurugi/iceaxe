package com.tsurugidb.iceaxe.example;

import java.net.URI;

import com.tsurugidb.iceaxe.TsurugiConnector;

/**
 * TsurugiConnector example
 */
@SuppressWarnings("unused")
public class Example01Connector {

    void connectorString() {
        var connector = TsurugiConnector.createConnector("tcp://localhost:12345");
//      connector.createSession()
    }

    void connectorUri() {
        var endpoint = URI.create("tcp://localhost:12345");
        var connector = TsurugiConnector.createConnector(endpoint);
//      connector.createSession()
    }
}
