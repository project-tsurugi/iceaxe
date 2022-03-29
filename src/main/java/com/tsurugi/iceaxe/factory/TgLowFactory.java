package com.tsurugi.iceaxe.factory;

import java.io.IOException;
import java.util.concurrent.Future;

import com.nautilus_technologies.tsubakuro.channel.common.connection.Connector;
import com.nautilus_technologies.tsubakuro.channel.common.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.low.sql.Session;

/**
 * Tsurugi Factory for Low-level-API
 */
public abstract class TgLowFactory {

    public abstract Connector createConnector(String name);

    public abstract Session createSession();

    public Future<SessionWire> createSessionWire(Connector connector) throws IOException {
        return connector.connect();
    }
}
