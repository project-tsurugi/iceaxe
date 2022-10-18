package com.tsurugidb.iceaxe.statement;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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

    protected TsurugiPreparedStatement(@Nonnull TsurugiSession session, @Nonnull String sql) {
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

    /**
     * get SQL.
     *
     * @return SQL
     */
    @Nonnull
    public String getSql() {
        return this.sql;
    }

    protected final IceaxeConvertUtil getConvertUtil(@Nullable IceaxeConvertUtil primaryConvertUtil) {
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

    @Override
    public String toString() {
        var name = getClass().getSimpleName();
        if (name == null) {
            name = TsurugiPreparedStatement.class.getSimpleName();
        }
        return name + "[" + sql + "]"; //$NON-NLS-1$ //$NON-NLS-2$
    }
}
