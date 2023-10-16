package com.tsurugidb.iceaxe.sql;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;

import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.TgTimeValue;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi SQL definition.
 */
public abstract class TsurugiSql implements AutoCloseable {

    private static final AtomicInteger SQL_DEFINITION_COUNT = new AtomicInteger(0);
    private static final AtomicInteger SQL_EXECUTE_COUNT = new AtomicInteger(0);

    private final int iceaxeSqlId;
    private final TsurugiSession ownerSession;
    /** SQL */
    protected final String sql;
    private IceaxeTimeout explainConnectTimeout;
    private IceaxeTimeout explainCloseTimeout;
    private volatile boolean closed = false;

    /**
     * Creates a new instance.
     *
     * @param session session
     * @param sql     SQL
     * @throws IOException if an I/O error occurs while disposing the resources
     */
    protected TsurugiSql(@Nonnull TsurugiSession session, @Nonnull String sql) throws IOException {
        this.iceaxeSqlId = SQL_DEFINITION_COUNT.incrementAndGet();
        this.ownerSession = Objects.requireNonNull(session);
        this.sql = Objects.requireNonNull(sql);
    }

    /**
     * initialize.
     * <p>
     * call from constructor after applyCloseTimeout()
     * </p>
     *
     * @param future future to close when an error occurs
     * @throws IOException if session already closed
     */
    protected void initialize(FutureResponse<PreparedStatement> future) throws IOException {
        try {
            ownerSession.addChild(this);
        } catch (Throwable e) {
            var log = LoggerFactory.getLogger(getClass());
            log.trace("tsurugiSql.initialize close start", e);
            try {
                IceaxeIoUtil.close(future);
            } catch (Throwable c) {
                e.addSuppressed(c);
            }
            log.trace("tsurugiSql.initialize close end");
            throw e;
        }
    }

    /**
     * is prepared.
     *
     * @return true: prepared
     */
    public abstract boolean isPrepared();

    /**
     * get iceaxe sqlId.
     *
     * @return iceaxe sqlId
     */
    public int getIceaxeSqlId() {
        return this.iceaxeSqlId;
    }

    /**
     * get session.
     *
     * @return session
     */
    protected final TsurugiSession getSession() {
        return this.ownerSession;
    }

    /**
     * get session option.
     *
     * @return session option
     */
    protected final TgSessionOption getSessionOption() {
        return ownerSession.getSessionOption();
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
     * set explain-connect-timeout.
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setExplainConnectTimeout(long time, TimeUnit unit) {
        setExplainConnectTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set explain-connect-timeout.
     *
     * @param timeout time
     */
    public void setExplainConnectTimeout(TgTimeValue timeout) {
        getExplainConnectTimeout().set(timeout);
    }

    /**
     * get explain-connect-timeout.
     *
     * @return timeout
     */
    protected synchronized IceaxeTimeout getExplainConnectTimeout() {
        if (this.explainConnectTimeout == null) {
            var sessionOption = ownerSession.getSessionOption();
            this.explainConnectTimeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.EXPLAIN_CONNECT);
        }
        return this.explainConnectTimeout;
    }

    /**
     * set explain-close-timeout.
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setExplainCloseTimeout(long time, TimeUnit unit) {
        setExplainCloseTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set explain-close-timeout.
     *
     * @param timeout time
     */
    public void setExplainCloseTimeout(TgTimeValue timeout) {
        getExplainCloseTimeout().set(timeout);
    }

    /**
     * get explain-close-timeout.
     *
     * @return timeout
     */
    protected synchronized IceaxeTimeout getExplainCloseTimeout() {
        if (this.explainCloseTimeout == null) {
            var sessionOption = ownerSession.getSessionOption();
            this.explainCloseTimeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.EXPLAIN_CLOSE);
        }
        return this.explainCloseTimeout;
    }

    /**
     * get new iceaxe SQL executeId.
     *
     * @return iceaxe SQL executeId
     */
    protected final int getNewIceaxeSqlExecuteId() {
        return SQL_EXECUTE_COUNT.incrementAndGet();
    }

    /**
     * get convert type utility.
     *
     * @param primaryConvertUtil primary convert type utility
     * @return convert type utility
     */
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
    public void close() throws IOException, InterruptedException {
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

    /**
     * check close.
     *
     * @throws IOException if already closed
     */
    protected void checkClose() throws IOException {
        if (isClosed()) {
            throw new IceaxeIOException(IceaxeErrorCode.PS_ALREADY_CLOSED);
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
