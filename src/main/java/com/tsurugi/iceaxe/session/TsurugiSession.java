package com.tsurugi.iceaxe.session;

import java.io.IOException;
import java.util.concurrent.Future;

import com.nautilus_technologies.tsubakuro.channel.common.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.tsurugi.iceaxe.TsurugiConnector;
import com.tsurugi.iceaxe.factory.TgLowFactory;
import com.tsurugi.iceaxe.util.IceaxeFutureUtil;

/**
 * Tsurugi Session
 */
public class TsurugiSession {

    private final TsurugiConnector owerConnector;
    private final TgSessionInfo sessionInfo;
    private final Session lowSession;
    private Future<SessionWire> lowSessionWireFuture;

    // internal
    public TsurugiSession(TsurugiConnector connector, TgSessionInfo info, Session lowSession, Future<SessionWire> lowSessionWireFuture) {
        this.owerConnector = connector;
        this.sessionInfo = info;
        this.lowSession = lowSession;
        this.lowSessionWireFuture = lowSessionWireFuture;
    }

    // internal
    public final TgLowFactory getLowFactory() {
        return owerConnector.getLowFactory();
    }

    protected final synchronized Session getLowSession() throws IOException {
        if (this.lowSessionWireFuture != null) {
            SessionWire sessionWire = IceaxeFutureUtil.getFromFuture(lowSessionWireFuture, sessionInfo);
            lowSession.connect(sessionWire);
            this.lowSessionWireFuture = null;
        }
        return lowSession;
    }
}
