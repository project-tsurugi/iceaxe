package com.tsurugi.iceaxe.session;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;

import com.nautilus_technologies.tsubakuro.channel.common.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.tsurugi.iceaxe.util.IceaxeFutureUtil;

/**
 * Tsurugi Session
 */
public class TsurugiSession implements Closeable {

    private final TgSessionInfo sessionInfo;
    private final Session lowSession;
    private Future<SessionWire> lowSessionWireFuture;

    // internal
    public TsurugiSession(TgSessionInfo info, Session lowSession, Future<SessionWire> lowSessionWireFuture) {
        this.sessionInfo = info;
        this.lowSession = lowSession;
        this.lowSessionWireFuture = lowSessionWireFuture;

        lowSession.setCloseTimeout(info.timeoutTime(), info.timeoutUnit());
    }

    protected final synchronized Session getLowSession() throws IOException {
        if (this.lowSessionWireFuture != null) {
            var lowSessionWire = IceaxeFutureUtil.getFromFuture(lowSessionWireFuture, sessionInfo);
            lowSession.connect(lowSessionWire);
            this.lowSessionWireFuture = null;
        }
        return lowSession;
    }

    @Override
    public void close() throws IOException {
        try {
            getLowSession();
        } finally {
            lowSession.close();
        }
    }
}
