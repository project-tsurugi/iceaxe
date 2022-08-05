package com.tsurugidb.iceaxe.statement;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.TgTimeValue;
import com.tsurugidb.jogasaki.proto.SqlRequest.Parameter;

/**
 * Tsurugi PreparedStatement with Low-PreparedStatement
 * 
 * @param <P> parameter type
 */
public abstract class TsurugiPreparedStatementWithLowPs<P> extends TsurugiPreparedStatement {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private FutureResponse<PreparedStatement> lowPreparedStatementFuture;
    private PreparedStatement lowPreparedStatement;
    private final TgParameterMapping<P> parameterMapping;
    private final IceaxeTimeout connectTimeout;
    private final IceaxeTimeout closeTimeout;

    protected TsurugiPreparedStatementWithLowPs(TsurugiSession session, FutureResponse<PreparedStatement> lowPreparedStatementFuture, TgParameterMapping<P> parameterMapping) {
        super(session);
        this.lowPreparedStatementFuture = lowPreparedStatementFuture;
        this.parameterMapping = parameterMapping;
        var info = session.getSessionInfo();
        this.connectTimeout = new IceaxeTimeout(info, TgTimeoutKey.PS_CONNECT);
        this.closeTimeout = new IceaxeTimeout(info, TgTimeoutKey.PS_CLOSE);

        applyCloseTimeout();
    }

    private void applyCloseTimeout() {
        closeTimeout.apply(lowPreparedStatement);
        closeTimeout.apply(lowPreparedStatementFuture);
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
        connectTimeout.set(timeout);
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
        closeTimeout.set(timeout);

        applyCloseTimeout();
    }

//  @ThreadSafe
    protected final synchronized PreparedStatement getLowPreparedStatement() throws IOException {
        if (this.lowPreparedStatement == null) {
            log.trace("lowPreparedStatement get start");
            this.lowPreparedStatement = IceaxeIoUtil.getFromFuture(lowPreparedStatementFuture, connectTimeout);
            log.trace("lowPreparedStatement get end");
            try {
                IceaxeIoUtil.close(lowPreparedStatementFuture);
                this.lowPreparedStatementFuture = null;
            } finally {
                applyCloseTimeout();
            }
        }
        return this.lowPreparedStatement;
    }

    protected final List<Parameter> getLowParameterList(P parameter) {
        var convertUtil = getConvertUtil(parameterMapping.getConvertUtil());
        return parameterMapping.toLowParameterList(parameter, convertUtil);
    }

    @Override
    public void close() throws IOException {
        log.trace("lowPs close start");
        // not try-finally
        IceaxeIoUtil.close(lowPreparedStatement, lowPreparedStatementFuture);
        super.close();
        log.trace("lowPs close end");
    }
}
