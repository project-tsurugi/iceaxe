package com.tsurugidb.iceaxe.statement;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.exception.TsurugiPreparedStatementIOException;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;

/**
 * Tsurugi PreparedStatement
 */
public abstract class TsurugiPreparedStatement implements Closeable {

    private final TsurugiSession ownerSession;
    protected final String sql;
    private boolean closed = false;

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

    /**
     * Returns the closed state of the prepared statement.
     *
     * @return true if the prepared statement has been closed
     * @see #close()
     */
    public boolean isClosed() {
        return this.closed;
    }

    protected void checkClose() throws IOException {
        if (isClosed()) {
            throw new TsurugiPreparedStatementIOException(TsurugiPreparedStatementIOException.MESSAGE_ALREADY_CLOSED);
        }
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
        this.closed = true;
    }

    @Override
    public String toString() {
        var name = getClass().getSimpleName();
        if (name.isEmpty()) {
            name = TsurugiPreparedStatement.class.getSimpleName();
        }
        return name + "[" + sql + "]"; //$NON-NLS-1$ //$NON-NLS-2$
    }
}
