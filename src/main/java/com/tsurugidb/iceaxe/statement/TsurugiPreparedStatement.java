package com.tsurugidb.iceaxe.statement;

import java.io.Closeable;
import java.io.IOException;

import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;

/**
 * Tsurugi PreparedStatement
 */
public abstract class TsurugiPreparedStatement implements Closeable {

    private final TsurugiSession ownerSession;

    protected TsurugiPreparedStatement(TsurugiSession session) {
        this.ownerSession = session;
        session.addChild(this);
    }

    protected final TgSessionInfo getSessionInfo() {
        return ownerSession.getSessionInfo();
    }

    @Override
    public void close() throws IOException {
        ownerSession.removeChild(this);
    }
}
