package com.tsurugidb.iceaxe.statement;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.TgTimeValue;

/**
 * Tsurugi SQL statement
 */
public abstract class TsurugiSql implements Closeable {

    private final TsurugiSession ownerSession;
    protected final String sql;
    private IceaxeTimeout explainConnectTimeout;
    private IceaxeTimeout explainCloseTimeout;
    private boolean closed = false;

    protected TsurugiSql(@Nonnull TsurugiSession session, @Nonnull String sql) throws IOException {
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
     * set explain-connect-timeout
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setExplainConnectTimeout(long time, TimeUnit unit) {
        setExplainConnectTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set explain-connect-timeout
     *
     * @param timeout time
     */
    public void setExplainConnectTimeout(TgTimeValue timeout) {
        getExplainConnectTimeout().set(timeout);
    }

    protected synchronized IceaxeTimeout getExplainConnectTimeout() {
        if (this.explainConnectTimeout == null) {
            var info = ownerSession.getSessionInfo();
            this.explainConnectTimeout = new IceaxeTimeout(info, TgTimeoutKey.EXPLAIN_CONNECT);
        }
        return this.explainConnectTimeout;
    }

    /**
     * set explain-close-timeout
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setExplainCloseTimeout(long time, TimeUnit unit) {
        setExplainCloseTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set explain-close-timeout
     *
     * @param timeout time
     */
    public void setExplainCloseTimeout(TgTimeValue timeout) {
        getExplainCloseTimeout().set(timeout);
    }

    protected synchronized IceaxeTimeout getExplainCloseTimeout() {
        if (this.explainCloseTimeout == null) {
            var info = ownerSession.getSessionInfo();
            this.explainCloseTimeout = new IceaxeTimeout(info, TgTimeoutKey.EXPLAIN_CLOSE);
        }
        return this.explainCloseTimeout;
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

    /**
     * Returns the closed state of the prepared statement.
     *
     * @return {@code true} if the prepared statement has been closed
     * @see #close()
     */
    public boolean isClosed() {
        return this.closed;
    }

    protected void checkClose() throws IOException {
        if (isClosed()) {
            throw new TsurugiIOException(IceaxeErrorCode.PS_ALREADY_CLOSED);
        }
    }

    @Override
    public String toString() {
        var name = getClass().getSimpleName();
        if (name.isEmpty()) {
            name = TsurugiSql.class.getSimpleName();
        }
        return name + "[" + sql + "]"; //$NON-NLS-1$ //$NON-NLS-2$
    }
}