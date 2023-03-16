package com.tsurugidb.iceaxe.sql;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.OverridingMethodsMustInvokeSuper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.sql.explain.TgStatementMetadata;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.TgTimeValue;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi SQL prepared statement
 *
 * @param <P> parameter type
 */
public abstract class TsurugiSqlPrepared<P> extends TsurugiSql {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private FutureResponse<PreparedStatement> lowPreparedStatementFuture;
    private Throwable lowFutureException = null;
    private PreparedStatement lowPreparedStatement;
    private final TgParameterMapping<P> parameterMapping;
    private final IceaxeTimeout connectTimeout;
    private final IceaxeTimeout closeTimeout;

    protected TsurugiSqlPrepared(TsurugiSession session, String sql, FutureResponse<PreparedStatement> lowPreparedStatementFuture, TgParameterMapping<P> parameterMapping) throws IOException {
        super(session, sql);
        this.lowPreparedStatementFuture = lowPreparedStatementFuture;
        this.parameterMapping = parameterMapping;
        var sessionOption = session.getSessionOption();
        this.connectTimeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.PS_CONNECT);
        this.closeTimeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.PS_CLOSE);

        applyCloseTimeout();
    }

    @Override
    public final boolean isPrepared() {
        return true;
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
            if (lowFutureException != null) {
                throw new TsurugiIOException(IceaxeErrorCode.PS_LOW_ERROR, lowFutureException);
            }

            log.trace("lowPs get start");
            try {
                this.lowPreparedStatement = IceaxeIoUtil.getAndCloseFuture(lowPreparedStatementFuture, connectTimeout);
            } catch (Throwable e) {
                this.lowFutureException = e;
                throw e;
            }
            log.trace("lowPs get end");

            this.lowPreparedStatementFuture = null;
            applyCloseTimeout();
        }
        return this.lowPreparedStatement;
    }

    protected final List<Parameter> getLowParameterList(P parameter) {
        var convertUtil = getConvertUtil(parameterMapping.getConvertUtil());
        return parameterMapping.toLowParameterList(parameter, convertUtil);
    }

    /**
     * Retrieves execution plan of the statement.
     *
     * @param parameter SQL parameter
     * @return statement metadata
     * @throws IOException
     */
    public TgStatementMetadata explain(P parameter) throws IOException {
        var session = getSession();
        var lowPs = getLowPreparedStatement();
        var lowParameterList = getLowParameterList(parameter);

        var helper = session.getExplainHelper();
        return helper.explain(session, sql, parameter, lowPs, lowParameterList, getExplainConnectTimeout(), getExplainCloseTimeout());
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void close() throws IOException {
        log.trace("lowPs close start");
        // not try-finally
        IceaxeIoUtil.close(lowPreparedStatement, lowPreparedStatementFuture);
        super.close();
        log.trace("lowPs close end");
    }
}