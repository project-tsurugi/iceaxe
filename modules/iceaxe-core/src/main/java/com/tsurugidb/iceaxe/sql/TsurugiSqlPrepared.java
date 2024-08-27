/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.iceaxe.sql;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.explain.TgStatementMetadata;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.TgTimeValue;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi SQL definition (prepared).
 *
 * @param <P> parameter type
 */
public abstract class TsurugiSqlPrepared<P> extends TsurugiSql {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final TgParameterMapping<P> parameterMapping;
    private FutureResponse<PreparedStatement> lowPreparedStatementFuture;
    private Throwable lowFutureException = null;
    private PreparedStatement lowPreparedStatement;
    private final IceaxeTimeout connectTimeout;
    private final IceaxeTimeout closeTimeout;

    /**
     * Creates a new instance.
     * <p>
     * Call {@link #initialize(FutureResponse)} after construct.
     * </p>
     *
     * @param session          session
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     */
    @IceaxeInternal
    protected TsurugiSqlPrepared(TsurugiSession session, String sql, TgParameterMapping<P> parameterMapping) {
        super(session, sql);
        this.parameterMapping = parameterMapping;
        var sessionOption = session.getSessionOption();
        this.connectTimeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.PS_CONNECT);
        this.closeTimeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.PS_CLOSE);
    }

    /**
     * initialize.
     * <p>
     * Call this method only once after construct.
     * </p>
     *
     * @param lowPreparedStatementFuture future of prepared statement
     * @throws IOException if session already closed
     * @since 1.3.0
     */
    @IceaxeInternal
    public void initialize(FutureResponse<PreparedStatement> lowPreparedStatementFuture) throws IOException {
        if (this.lowPreparedStatementFuture != null || this.lowPreparedStatement != null) {
            throw new IllegalStateException("initialize() is already called");
        }

        this.lowPreparedStatementFuture = Objects.requireNonNull(lowPreparedStatementFuture);

        try {
            super.initialize();
        } catch (Throwable e) {
            log.trace("TsurugiSqlPrepared.initialize close start", e);
            try {
                IceaxeIoUtil.close(closeTimeout.getNanos(), IceaxeErrorCode.PS_CLOSE_TIMEOUT, IceaxeErrorCode.PS_CLOSE_ERROR, //
                        lowPreparedStatementFuture);
            } catch (Throwable c) {
                e.addSuppressed(c);
            }
            log.trace("TsurugiSqlPrepared.initialize close end");
            throw e;
        }
    }

    @Override
    public final boolean isPrepared() {
        return true;
    }

    /**
     * set connect-timeout.
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setConnectTimeout(long time, TimeUnit unit) {
        setConnectTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set connect-timeout.
     *
     * @param timeout time
     */
    public void setConnectTimeout(TgTimeValue timeout) {
        connectTimeout.set(timeout);
    }

    /**
     * set close-timeout.
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setCloseTimeout(long time, TimeUnit unit) {
        setCloseTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set close-timeout.
     *
     * @param timeout time
     */
    public void setCloseTimeout(TgTimeValue timeout) {
        closeTimeout.set(timeout);
    }

    /**
     * get low prepared statement.
     *
     * @return prepared statement
     * @throws IOException          if an I/O error occurs while retrieving prepared statement
     * @throws InterruptedException if interrupted while retrieving prepared statement
     */
    @IceaxeInternal
//  @ThreadSafe
    public final synchronized PreparedStatement getLowPreparedStatement() throws IOException, InterruptedException {
        if (this.lowPreparedStatement == null) {
            if (lowFutureException != null) {
                throw new IceaxeIOException(IceaxeErrorCode.PS_LOW_ERROR, lowFutureException);
            }
            if (this.lowPreparedStatementFuture == null) {
                throw new IllegalStateException("initialize() is not called");
            }

            log.trace("lowPs get start");
            try {
                this.lowPreparedStatement = IceaxeIoUtil.getAndCloseFuture(lowPreparedStatementFuture, //
                        connectTimeout, IceaxeErrorCode.PS_CONNECT_TIMEOUT, //
                        IceaxeErrorCode.PS_CLOSE_TIMEOUT);
            } catch (Throwable e) {
                this.lowFutureException = e;
                throw e;
            }
            log.trace("lowPs get end");

            this.lowPreparedStatementFuture = null;
        }
        return this.lowPreparedStatement;
    }

    /**
     * get low parameter list.
     *
     * @param parameter SQL parameter
     * @return list of parameter
     */
    protected final List<Parameter> getLowParameterList(P parameter) {
        var convertUtil = getConvertUtil(parameterMapping.getConvertUtil());
        return parameterMapping.toLowParameterList(parameter, convertUtil);
    }

    /**
     * Retrieves execution plan of the statement.
     *
     * @param parameter SQL parameter
     * @return statement metadata
     * @throws IOException          if an I/O error occurs while retrieving statement metadata
     * @throws InterruptedException if interrupted while retrieving statement metadata
     */
    public TgStatementMetadata explain(P parameter) throws IOException, InterruptedException {
        var session = getSession();
        var lowPs = getLowPreparedStatement();
        var lowParameterList = getLowParameterList(parameter);

        var helper = session.getExplainHelper();
        var connectTimeout = getExplainConnectTimeout();
        @SuppressWarnings("deprecation")
        var closeTimeout = getExplainCloseTimeout();
        return helper.explain(session, sql, parameter, lowPs, lowParameterList, connectTimeout, closeTimeout);
    }

    // close

    @Override
//  @OverridingMethodsMustInvokeSuper
    public void close() throws IOException, InterruptedException {
        close(closeTimeout.getNanos());
    }

    @Override
    public void close(long timeoutNanos) throws IOException, InterruptedException {
        log.trace("lowPs close start");
        // not try-finally
        IceaxeIoUtil.close(timeoutNanos, IceaxeErrorCode.PS_CLOSE_TIMEOUT, IceaxeErrorCode.PS_CLOSE_ERROR, //
                lowPreparedStatement, lowPreparedStatementFuture);
        super.close();
        log.trace("lowPs close end");
    }
}
