package com.tsurugi.iceaxe.session;

import java.io.Closeable;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Future;

import com.nautilus_technologies.tsubakuro.channel.common.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.tsurugi.iceaxe.statement.TgParameter;
import com.tsurugi.iceaxe.statement.TgVariable;
import com.tsurugi.iceaxe.statement.TsurugiPreparedStatement;
import com.tsurugi.iceaxe.statement.TsurugiResultRecord;
import com.tsurugi.iceaxe.util.IceaxeIoUtil;

/**
 * Tsurugi Session
 */
public class TsurugiSession implements Closeable {

    private final TgSessionInfo sessionInfo;
    private final Session lowSession;
    private Future<SessionWire> lowSessionWireFuture;
    private final NavigableSet<Closeable> closeableSet = new ConcurrentSkipListSet<>();

    // internal
    public TsurugiSession(TgSessionInfo info, Session lowSession, Future<SessionWire> lowSessionWireFuture) {
        this.sessionInfo = info;
        this.lowSession = lowSession;
        this.lowSessionWireFuture = lowSessionWireFuture;

        lowSession.setCloseTimeout(info.timeoutTime(), info.timeoutUnit());
    }

    public TgSessionInfo getSessionInfo() {
        return sessionInfo;
    }

    protected final synchronized Session getLowSession() throws IOException {
        if (this.lowSessionWireFuture != null) {
            var lowSessionWire = IceaxeIoUtil.getFromFuture(lowSessionWireFuture, sessionInfo);
            lowSession.connect(lowSessionWire);
            this.lowSessionWireFuture = null;
        }
        return lowSession;
    }

    public TsurugiPreparedStatement<TgParameter, TsurugiResultRecord> createPreparedStatement(String sql, TgVariable variable) throws IOException {
        var lowPreparedStatementFuture = getLowSession().prepare(sql, variable.toLowPlaceHolder());
        var ps = new TsurugiPreparedStatement<TgParameter, TsurugiResultRecord>(this, lowPreparedStatementFuture);
        closeableSet.add(ps);
        return ps;
    }

    // internal
    public void removeChild(Closeable closeable) {
        closeableSet.remove(closeable);
    }

    @Override
    public void close() throws IOException {
        IceaxeIoUtil.close(closeableSet, () -> {
            try {
                getLowSession();
            } finally {
                lowSession.close();
            }
        });
    }
}
