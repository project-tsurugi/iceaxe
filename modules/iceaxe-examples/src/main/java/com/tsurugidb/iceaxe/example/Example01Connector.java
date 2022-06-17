package com.tsurugidb.iceaxe.example;

import java.net.URI;

import com.tsurugidb.iceaxe.TsurugiConnector;

/**
 * TsurugiConnector example
 */
@SuppressWarnings("unused")
public class Example01Connector {

    void connectorString() {
        var connector = TsurugiConnector.createConnector("dbname");
//      connector.createSession()
    }

    void connectorUri() {
        var endpoint = URI.create("dbname");
        var connector = TsurugiConnector.createConnector(endpoint);
//      connector.createSession()
    }
}
