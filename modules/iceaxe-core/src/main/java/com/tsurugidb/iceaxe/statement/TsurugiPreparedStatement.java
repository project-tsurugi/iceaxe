package com.tsurugidb.iceaxe.statement;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;

/**
 * Tsurugi PreparedStatement
 */
public abstract class TsurugiPreparedStatement implements Closeable {

    private final TsurugiSession ownerSession;
    protected final String sql;

    protected TsurugiPreparedStatement(TsurugiSession session, String sql) {
        this.ownerSession = session;
        this.sql = sql;
        session.addChild(this);
    }

    protected final TsurugiSession getSession() {
        return this.ownerSession;
    }

    protected final TgSessionInfo getSessionInfo() {
        return ownerSession.getSessionInfo();
    }

    protected final IceaxeConvertUtil getConvertUtil(IceaxeConvertUtil primaryConvertUtil) {
        var convertUtil = primaryConvertUtil;
        if (convertUtil == null) {
            convertUtil = ownerSession.getConvertUtil();
        }
        if (convertUtil == null) {
            convertUtil = IceaxeConvertUtil.INSTANCE;
        }
        return convertUtil;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void close() throws IOException {
        ownerSession.removeChild(this);
    }
}
