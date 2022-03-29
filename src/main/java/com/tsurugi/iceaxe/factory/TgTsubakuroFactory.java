package com.tsurugi.iceaxe.factory;

import com.nautilus_technologies.tsubakuro.channel.common.connection.Connector;
import com.nautilus_technologies.tsubakuro.channel.common.connection.ConnectorImpl;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SessionImpl;
import com.nautilus_technologies.tsubakuro.low.sql.Session;

/**
 * Tsurugi Factory for tsubakuro
 */
public class TgTsubakuroFactory extends TgLowFactory {

    @Override
    public Connector createConnector(String name) {
        return new ConnectorImpl(name);
    }

    @Override
    public Session createSession() {
        return new SessionImpl();
    }
}
