package com.tsurugi.iceaxe.statement;

import java.io.Closeable;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;

import com.tsurugi.iceaxe.session.TgSessionInfo;
import com.tsurugi.iceaxe.session.TsurugiSession;
import com.tsurugi.iceaxe.util.IceaxeIoUtil;

/**
 * Tsurugi PreparedStatement
 */
public abstract class TsurugiPreparedStatement implements Closeable {

    private final TsurugiSession ownerSession;
    private final NavigableSet<Closeable> closeableSet = new ConcurrentSkipListSet<>();

    protected TsurugiPreparedStatement(TsurugiSession session) {
        this.ownerSession = session;
    }

    public final TgSessionInfo getSessionInfo() {
        return ownerSession.getSessionInfo();
    }

    protected final void addChild(Closeable closeable) {
        closeableSet.add(closeable);
    }

    // internal
    public void removeChild(Closeable closeable) {
        closeableSet.remove(closeable);
    }

    @Override
    public void close() throws IOException {
        IceaxeIoUtil.close(closeableSet, () -> {
            ownerSession.removeChild(this);
        });
    }
}
