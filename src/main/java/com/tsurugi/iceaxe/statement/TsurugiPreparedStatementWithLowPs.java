package com.tsurugi.iceaxe.statement;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos.ParameterSet;
import com.tsurugi.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugi.iceaxe.session.TsurugiSession;
import com.tsurugi.iceaxe.util.IceaxeIoUtil;
import com.tsurugi.iceaxe.util.TgTimeValue;

/**
 * Tsurugi PreparedStatement with Low-PreparedStatement
 * 
 * @param <P> parameter type
 */
public abstract class TsurugiPreparedStatementWithLowPs<P> extends TsurugiPreparedStatement {

    private Future<PreparedStatement> lowPreparedStatementFuture;
    private PreparedStatement lowPreparedStatement;
    private final TgParameterMapping<P> parameterMapping;
    private TgTimeValue connectTimeout;
    private TgTimeValue closeTimeout;

    protected TsurugiPreparedStatementWithLowPs(TsurugiSession session, Future<PreparedStatement> lowPreparedStatementFuture, TgParameterMapping<P> parameterMapping) {
        super(session);
        this.lowPreparedStatementFuture = lowPreparedStatementFuture;
        this.parameterMapping = parameterMapping;
        setConnectTimeout(session.getSessionInfo().timeout(TgTimeoutKey.PS_CONNECT));
        setCloseTimeout(session.getSessionInfo().timeout(TgTimeoutKey.PS_CLOSE));
    }

    /**
     * set connect-timeout
     * 
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setConnectTimeout(long time, TimeUnit unit) {
        setConnectTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set connect-timeout
     * 
     * @param timeout time
     */
    public void setConnectTimeout(TgTimeValue timeout) {
        this.connectTimeout = timeout;
    }

    /**
     * set close-timeout
     * 
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setCloseTimeout(long time, TimeUnit unit) {
        setCloseTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set close-timeout
     * 
     * @param timeout time
     */
    public void setCloseTimeout(TgTimeValue timeout) {
        this.closeTimeout = timeout;
        setCloseTimeoutInternal();
    }

    protected synchronized final PreparedStatement getLowPreparedStatement() throws IOException {
        if (this.lowPreparedStatement == null) {
            this.lowPreparedStatement = IceaxeIoUtil.getFromFuture(lowPreparedStatementFuture, connectTimeout);
            setCloseTimeoutInternal();
            this.lowPreparedStatementFuture = null;
        }
        return this.lowPreparedStatement;
    }

    private void setCloseTimeoutInternal() {
        if (this.lowPreparedStatement != null) {
            lowPreparedStatement.setCloseTimeout(closeTimeout.value(), closeTimeout.unit());
        }
    }

    protected final ParameterSet getLowParameterSet(P parameter) {
        return parameterMapping.toLowParameterSet(parameter);
    }

    @Override
    public void close() throws IOException {
        // not try-finally
        getLowPreparedStatement().close();
        super.close();
    }
}
