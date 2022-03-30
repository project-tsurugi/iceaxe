package com.tsurugi.iceaxe;

import java.io.IOException;

import com.nautilus_technologies.tsubakuro.channel.common.connection.Connector;
import com.nautilus_technologies.tsubakuro.channel.common.connection.ConnectorImpl;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SessionImpl;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.tsurugi.iceaxe.session.TgSessionInfo;
import com.tsurugi.iceaxe.session.TsurugiSession;

/**
 * Tsurugi Connector
 */
public class TsurugiConnector {

    /**
     * create Tsurugi Connector
     * 
     * @param name database name
     * @return Tsurugi Connector
     */
    public static TsurugiConnector createConnector(String name) {
        var lowConnector = new ConnectorImpl(name);
        var connector = new TsurugiConnector(lowConnector);
        return connector;
    }

    private final Connector lowConnector;

    protected TsurugiConnector(Connector lowConnector) {
        this.lowConnector = lowConnector;
    }

    /**
     * create Tsurugi Session
     * 
     * @param info Session Information
     * @return Tsurugi Session
     * @throws IOException
     */
    public TsurugiSession createSession(TgSessionInfo info) throws IOException {
        var lowSession = createLowSession();
        var lowSessionWireFuture = lowConnector.connect();
        var session = new TsurugiSession(info, lowSession, lowSessionWireFuture);
        return session;
    }

    protected Session createLowSession() {
        return new SessionImpl();
    }
}
