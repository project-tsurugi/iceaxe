package com.tsurugi.iceaxe;

import java.io.IOException;

import com.nautilus_technologies.tsubakuro.channel.common.connection.Connector;
import com.tsurugi.iceaxe.factory.TgLowFactory;
import com.tsurugi.iceaxe.factory.TgTsubakuroFactory;
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
        var lowFactory = new TgTsubakuroFactory();
        var lowConnector = lowFactory.createConnector(name);
        var connector = new TsurugiConnector(lowConnector, lowFactory);
        return connector;
    }

    private final Connector lowConnector;
    private final TgLowFactory lowFactory;

    protected TsurugiConnector(Connector lowConnector, TgLowFactory lowFactory) {
        this.lowConnector = lowConnector;
        this.lowFactory = lowFactory;
    }

    // internal
    public final TgLowFactory getLowFactory() {
        return lowFactory;
    }

    /**
     * create Tsurugi Session
     * 
     * @param info Session Information
     * @return Tsurugi Session
     * @throws IOException
     */
    public TsurugiSession createSession(TgSessionInfo info) throws IOException {
        var lowSession = getLowFactory().createSession();
        var lowSessionWireFuture = getLowFactory().createSessionWire(lowConnector);
        var session = new TsurugiSession(this, info, lowSession, lowSessionWireFuture);
        return session;
    }
}
