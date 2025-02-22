/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.iceaxe.session;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.exception.IceaxeTimeoutIOException;
import com.tsurugidb.iceaxe.metadata.TgTableMetadata;
import com.tsurugidb.iceaxe.metadata.TsurugiTableListHelper;
import com.tsurugidb.iceaxe.metadata.TsurugiTableMetadataHelper;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.event.TsurugiSessionEventListener;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlStatement;
import com.tsurugidb.iceaxe.sql.explain.TsurugiExplainHelper;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.transaction.status.TsurugiTransactionStatusHelper;
import com.tsurugidb.iceaxe.util.IceaxeCloseableSet;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.IceaxeTimeoutCloseable;
import com.tsurugidb.iceaxe.util.TgTimeValue;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.ShutdownType;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi Session.
 */
public class TsurugiSession implements IceaxeTimeoutCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiSession.class);

    private final TgSessionOption sessionOption;
    private FutureResponse<? extends Session> lowSessionFuture;
    private Session lowSession;
    private Throwable lowFutureException = null;
    private SqlClient lowSqlClient;
    private TsurugiTableListHelper tableListHelper = null;
    private TsurugiTableMetadataHelper tableMetadataHelper = null;
    private TsurugiExplainHelper explainHelper = null;
    private TsurugiTransactionStatusHelper txStatusHelper = null;
    private IceaxeConvertUtil convertUtil = null;
    private final IceaxeTimeout connectTimeout;
    private final IceaxeTimeout closeTimeout;
    private List<TsurugiSessionEventListener> eventListenerList = null;
    private final IceaxeCloseableSet closeableSet = new IceaxeCloseableSet();
    private TgSessionShutdownType closeShutdownType = null;
    private volatile boolean closed = false;

    /**
     * Creates a new instance.
     *
     * @param lowSessionFuture future of session
     * @param sessionOption    session option
     */
    @IceaxeInternal
    public TsurugiSession(FutureResponse<? extends Session> lowSessionFuture, TgSessionOption sessionOption) {
        this.sessionOption = Objects.requireNonNull(sessionOption);
        this.lowSessionFuture = lowSessionFuture;
        this.connectTimeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.SESSION_CONNECT);
        this.closeTimeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.SESSION_CLOSE);
    }

    /**
     * set convert type utility.
     *
     * @param convertUtil convert type utility
     */
    public void setConvertUtil(IceaxeConvertUtil convertUtil) {
        this.convertUtil = convertUtil;
    }

    /**
     * get convert type utility.
     *
     * @return convert type utility
     */
    public IceaxeConvertUtil getConvertUtil() {
        return this.convertUtil;
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
     * set shutdown type on close.
     *
     * @param shutdownType shutdown type
     * @since 1.4.0
     */
    public void setCloseShutdownType(TgSessionShutdownType shutdownType) {
        this.closeShutdownType = shutdownType;
    }

    /**
     * get shutdown type on close.
     *
     * @return shutdown type
     * @since 1.4.0
     */
    public TgSessionShutdownType getCloseShutdownType() {
        var shutdownType = this.closeShutdownType;
        if (shutdownType == null) {
            shutdownType = sessionOption.getCloseShutdownType();
        }
        return shutdownType;
    }

    /**
     * get session option.
     *
     * @return session option
     */
    public final @Nonnull TgSessionOption getSessionOption() {
        return this.sessionOption;
    }

    /**
     * add event listener.
     *
     * @param listener event listener
     * @return this
     */
    public TsurugiSession addEventListener(TsurugiSessionEventListener listener) {
        if (this.eventListenerList == null) {
            this.eventListenerList = new ArrayList<>();
        }
        eventListenerList.add(listener);
        return this;
    }

    /**
     * find event listener.
     *
     * @param predicate predicate for event listener
     * @return event listener
     * @since 1.3.0
     */
    public Optional<TsurugiSessionEventListener> findEventListener(Predicate<TsurugiSessionEventListener> predicate) {
        var listenerList = this.eventListenerList;
        if (listenerList != null) {
            for (var listener : listenerList) {
                if (predicate.test(listener)) {
                    return Optional.of(listener);
                }
            }
        }
        return Optional.empty();
    }

    private void event(Throwable occurred, Consumer<TsurugiSessionEventListener> action) {
        var listenerList = this.eventListenerList;
        if (listenerList != null) {
            try {
                for (var listener : listenerList) {
                    action.accept(listener);
                }
            } catch (Throwable e) {
                if (occurred != null) {
                    e.addSuppressed(occurred);
                }
                throw e;
            }
        }
    }

    /**
     * get low SQL client.
     *
     * @return low SQL client
     * @throws IOException          if an I/O error occurs while communicating to the server
     * @throws InterruptedException if interrupted while communicating to the server
     */
    @IceaxeInternal
//  @ThreadSafe
    public final synchronized SqlClient getLowSqlClient() throws IOException, InterruptedException {
        if (this.lowSqlClient == null) {
            var lowSession = getLowSession();
            LOG.trace("SqlClient.attach start");
            this.lowSqlClient = newSqlClient(lowSession);
            LOG.trace("SqlClient.attach end");
        }
        return this.lowSqlClient;
    }

    /**
     * create SqlClient instance.
     *
     * @param lowSession session
     * @return SqlClient
     * @since 1.4.0
     */
    protected SqlClient newSqlClient(Session lowSession) {
        return SqlClient.attach(lowSession);
    }

    /**
     * get session.
     *
     * @return session
     * @throws IOException          if an I/O error occurs while communicating to the server
     * @throws InterruptedException if interrupted while communicating to the server
     */
    @IceaxeInternal
//  @ThreadSafe
    public final synchronized Session getLowSession() throws IOException, InterruptedException {
        return getLowSession(connectTimeout, null);
    }

    private Session getLowSession(IceaxeTimeout timeout, @Nullable IceaxeErrorCode timeoutErrorCode) throws IOException, InterruptedException {
        if (this.lowSession == null) {
            if (this.lowFutureException != null) {
                throw new IceaxeIOException(IceaxeErrorCode.SESSION_LOW_ERROR, lowFutureException);
            }

            LOG.trace("lowSession get start");
            try {
                this.lowSession = IceaxeIoUtil.getAndCloseFuture(lowSessionFuture, //
                        timeout, IceaxeErrorCode.SESSION_CONNECT_TIMEOUT, //
                        IceaxeErrorCode.SESSION_CLOSE_TIMEOUT);
            } catch (IceaxeIOException e) {
                IceaxeErrorCode code = e.getDiagnosticCode();
                if (code.isTimeout() && timeoutErrorCode != null) {
                    e = new IceaxeTimeoutIOException(timeoutErrorCode, e);
                }
                this.lowFutureException = e;
                throw e;
            } catch (Throwable e) {
                this.lowFutureException = e;
                throw e;
            }
            LOG.trace("lowSession get end");

            this.lowSessionFuture = null;
        }
        return this.lowSession;

    }

    /**
     * Provide dead/alive information of the server to which the session is connected.
     *
     * @return {@code true} when the server is alive
     */
    public boolean isAlive() {
        try {
            return getLowSession().isAlive();
        } catch (IOException | InterruptedException e) {
            LOG.trace("exception in isAlive()", e);
            return false;
        }
    }

    /**
     * set TsurugiTableListHelper.
     *
     * @param helper TsurugiTableListHelper
     */
    public void setTableListHelper(TsurugiTableListHelper helper) {
        this.tableListHelper = helper;
    }

    /**
     * get TsurugiTableListHelper.
     *
     * @return TsurugiTableListHelper
     */
    public TsurugiTableListHelper getTableListHelper() {
        if (this.tableListHelper == null) {
            this.tableListHelper = new TsurugiTableListHelper();
        }
        return this.tableListHelper;
    }

    /**
     * get table names.
     *
     * @return table names
     * @throws IOException          if an I/O error occurs while retrieving table list
     * @throws InterruptedException if interrupted while retrieving table list
     */
//  @ThreadSafe
    public List<String> getTableNameList() throws IOException, InterruptedException {
        var helper = getTableListHelper();
        var tableList = helper.getTableList(this);
        return tableList.getTableNameList();
    }

    /**
     * set TableMetadataHelper.
     *
     * @param helper TableMetadataHelper
     */
    public void setTableMetadataHelper(TsurugiTableMetadataHelper helper) {
        this.tableMetadataHelper = helper;
    }

    /**
     * get TableMetadataHelper.
     *
     * @return TableMetadataHelper
     */
    public TsurugiTableMetadataHelper getTableMetadataHelper() {
        if (this.tableMetadataHelper == null) {
            this.tableMetadataHelper = new TsurugiTableMetadataHelper();
        }
        return this.tableMetadataHelper;
    }

    /**
     * get table metadata.
     *
     * @param tableName table name
     * @return table metadata (empty if table not found)
     * @throws IOException          if an I/O error occurs while retrieving table metadata
     * @throws InterruptedException if interrupted while retrieving table metadata
     */
//  @ThreadSafe
    public Optional<TgTableMetadata> findTableMetadata(String tableName) throws IOException, InterruptedException {
        var helper = getTableMetadataHelper();
        return helper.findTableMetadata(this, tableName);
    }

    /**
     * set ExplainHelper.
     *
     * @param helper ExplainHelper
     */
    public void setExplainHelper(TsurugiExplainHelper helper) {
        this.explainHelper = helper;
    }

    /**
     * get ExplainHelper.
     *
     * @return ExplainHelper
     */
    public TsurugiExplainHelper getExplainHelper() {
        if (this.explainHelper == null) {
            this.explainHelper = new TsurugiExplainHelper();
        }
        return this.explainHelper;
    }

    /**
     * set TransactionStatusHelper.
     *
     * @param helper TransactionStatusHelper
     */
    public void setTransactionStatusHelper(TsurugiTransactionStatusHelper helper) {
        this.txStatusHelper = helper;
    }

    /**
     * get TransactionStatusHelper.
     *
     * @return TransactionStatusHelper
     */
    public TsurugiTransactionStatusHelper getTransactionStatusHelper() {
        if (this.txStatusHelper == null) {
            this.txStatusHelper = new TsurugiTransactionStatusHelper();
        }
        return this.txStatusHelper;
    }

    /**
     * create SQL query.
     *
     * @param sql SQL
     * @return SQL query
     * @throws IOException if an I/O error occurs while create query
     */
//  @ThreadSafe
    public TsurugiSqlQuery<TsurugiResultEntity> createQuery(String sql) throws IOException {
        return createQuery(sql, TgResultMapping.DEFAULT);
    }

    /**
     * create SQL query.
     *
     * @param <R>           result type
     * @param sql           SQL
     * @param resultMapping result mapping
     * @return SQL query
     * @throws IOException if an I/O error occurs while create query
     */
//  @ThreadSafe
    public <R> TsurugiSqlQuery<R> createQuery(String sql, TgResultMapping<R> resultMapping) throws IOException {
        checkClose();
        LOG.trace("createQuery. sql={}", sql);
        var ps = new TsurugiSqlQuery<>(this, sql, resultMapping);
        ps.initialize();
        event(null, listener -> listener.createQuery(ps));
        return ps;
    }

    /**
     * create SQL prepared query.
     *
     * @param <P>              parameter type
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @return SQL prepared query
     * @throws IOException          if an I/O error occurs while create prepared query
     * @throws InterruptedException if interrupted while create prepared query
     */
//  @ThreadSafe
    public <P> TsurugiSqlPreparedQuery<P, TsurugiResultEntity> createQuery(String sql, TgParameterMapping<P> parameterMapping) throws IOException, InterruptedException {
        return createQuery(sql, parameterMapping, TgResultMapping.DEFAULT);
    }

    /**
     * create SQL prepared query.
     *
     * @param <P>              parameter type
     * @param <R>              result type
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @param resultMapping    result mapping
     * @return SQL prepared query
     * @throws IOException          if an I/O error occurs while create prepared query
     * @throws InterruptedException if interrupted while create prepared query
     */
//  @ThreadSafe
    public <P, R> TsurugiSqlPreparedQuery<P, R> createQuery(String sql, TgParameterMapping<P> parameterMapping, TgResultMapping<R> resultMapping) throws IOException, InterruptedException {
        checkClose();
        LOG.trace("createQuery start. sql={}", sql);
        var lowPlaceholderList = parameterMapping.toLowPlaceholderList();
        var lowPreparedStatementFuture = getLowSqlClient().prepare(sql, lowPlaceholderList);
        LOG.trace("createQuery started");
        var ps = new TsurugiSqlPreparedQuery<>(this, sql, parameterMapping, resultMapping);
        ps.initialize(lowPreparedStatementFuture);
        event(null, listener -> listener.createQuery(ps));
        return ps;
    }

    /**
     * create SQL statement.
     *
     * @param sql SQL
     * @return SQL statement
     * @throws IOException if an I/O error occurs while create statement
     */
//  @ThreadSafe
    public TsurugiSqlStatement createStatement(String sql) throws IOException {
        checkClose();
        LOG.trace("createStatement. sql={}", sql);
        var ps = new TsurugiSqlStatement(this, sql);
        ps.initialize();
        event(null, listener -> listener.createStatement(ps));
        return ps;
    }

    /**
     * create SQL prepared statement.
     *
     * @param <P>              parameter type
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @return SQL prepared statement
     * @throws IOException          if an I/O error occurs while create prepared statement
     * @throws InterruptedException if interrupted while create prepared statement
     */
//  @ThreadSafe
    public <P> TsurugiSqlPreparedStatement<P> createStatement(String sql, TgParameterMapping<P> parameterMapping) throws IOException, InterruptedException {
        checkClose();
        LOG.trace("createStatement start. sql={}", sql);
        var lowPlaceholderList = parameterMapping.toLowPlaceholderList();
        var lowPreparedStatementFuture = getLowSqlClient().prepare(sql, lowPlaceholderList);
        LOG.trace("createStatement started");
        var ps = new TsurugiSqlPreparedStatement<>(this, sql, parameterMapping);
        ps.initialize(lowPreparedStatementFuture);
        event(null, listener -> listener.createStatement(ps));
        return ps;
    }

    /**
     * create transaction manager.
     *
     * @return Transaction Manager
     */
//  @ThreadSafe
    public TsurugiTransactionManager createTransactionManager() {
        return createTransactionManager((TgTmSetting) null);
    }

    /**
     * create transaction manager.
     *
     * @param setting transaction manager settings
     * @return Transaction Manager
     */
//  @ThreadSafe
    public TsurugiTransactionManager createTransactionManager(TgTmSetting setting) {
        var tm = new TsurugiTransactionManager(this, setting);
        event(null, listener -> listener.createTransactionManager(tm));
        return tm;
    }

    /**
     * create transaction manager.
     *
     * @param txOption transaction option
     * @return Transaction Manager
     */
//  @ThreadSafe
    public TsurugiTransactionManager createTransactionManager(TgTxOption txOption) {
        var setting = TgTmSetting.of(txOption);
        return createTransactionManager(setting);
    }

    /**
     * create transaction.
     *
     * @param txOption transaction option
     * @return transaction
     * @throws IOException          if an I/O error occurs while create transaction
     * @throws InterruptedException if interrupted while create transaction
     */
//  @ThreadSafe
    public TsurugiTransaction createTransaction(@Nonnull TgTxOption txOption) throws IOException, InterruptedException {
        return createTransaction(txOption, null);
    }

    /**
     * create transaction.
     *
     * @param txOption    transaction option
     * @param initializer transaction initializer
     * @return transaction
     * @throws IOException          if an I/O error occurs while create transaction
     * @throws InterruptedException if interrupted while create transaction
     */
//  @ThreadSafe
    public TsurugiTransaction createTransaction(@Nonnull TgTxOption txOption, @Nullable Consumer<TsurugiTransaction> initializer) throws IOException, InterruptedException {
        checkClose();

        var lowOption = txOption.toLowTransactionOption();
        LOG.trace("lowTransaction create start. lowOption={}", lowOption);
        var lowTransactionFuture = getLowSqlClient().createTransaction(lowOption);
        LOG.trace("lowTransaction create started");
        var transaction = newTsurugiTransaction(txOption);
        transaction.initialize(lowTransactionFuture);
        if (initializer != null) {
            initializer.accept(transaction);
        }
        event(null, listener -> listener.createTransaction(transaction));
        return transaction;
    }

    /**
     * create transaction instance.
     *
     * @param txOption transaction option
     * @return transaction
     * @since 1.4.0
     */
    protected TsurugiTransaction newTsurugiTransaction(TgTxOption txOption) {
        return new TsurugiTransaction(this, txOption);
    }

    // child

    /**
     * add child object.
     *
     * @param closeable child object
     * @throws IOException if already closed
     */
    @IceaxeInternal
    public void addChild(IceaxeTimeoutCloseable closeable) throws IOException {
        checkClose();
        closeableSet.add(closeable);
    }

    /**
     * remove child object.
     *
     * @param closeable child object
     */
    @IceaxeInternal
    public void removeChild(IceaxeTimeoutCloseable closeable) {
        closeableSet.remove(closeable);
    }

    // close

    /**
     * shutdown this session.
     *
     * @param shutdownType shutdown type
     * @param time         timeout time
     * @param unit         timeout unit
     * @throws IOException          if an I/O error occurs during shutdown
     * @throws InterruptedException if interrupted during shutdown
     * @since 1.4.0
     */
    public void shutdown(TgSessionShutdownType shutdownType, long time, TimeUnit unit) throws IOException, InterruptedException {
        checkClose();
        shutdown(shutdownType, unit.toNanos(time));
    }

    /**
     * shutdown this session.
     *
     * @param shutdownType shutdown type
     * @param timeout      timeout
     * @throws IOException          if an I/O error occurs during shutdown
     * @throws InterruptedException if interrupted during shutdown
     * @since 1.4.0
     */
    public void shutdown(TgSessionShutdownType shutdownType, TgTimeValue timeout) throws IOException, InterruptedException {
        checkClose();
        shutdown(shutdownType, timeout.toNanos());
    }

    private void shutdown(TgSessionShutdownType shutdownType, long timeoutNanos) throws IOException, InterruptedException {
        LOG.trace("session shutdown start. shutdownType={}", shutdownType);
        Throwable occurred = null;
        try {
            IceaxeIoUtil.close(timeoutNanos, closeableSet, IceaxeErrorCode.SESSION_CHILD_CLOSE_ERROR, t -> {
                if (shutdownType == null) {
                    LOG.trace("do not shutdown. shutdownType=null");
                    return;
                }
                var lowShutdownType = shutdownType.getLowShutdownType();
                if (lowShutdownType == null) {
                    LOG.trace("do not shutdown. shutdownType={}", shutdownType);
                    return;
                }

                long start = System.nanoTime();
                var lowSession0 = getLowSession(new IceaxeTimeout(t, TimeUnit.NANOSECONDS), IceaxeErrorCode.SESSION_SHUTDOWN_TIMEOUT);
                var lowShutdownFuture = shutdownLow(lowSession0, lowShutdownType);

                var timeout = new IceaxeTimeout(IceaxeIoUtil.calculateTimeoutNanos(t, start), TimeUnit.NANOSECONDS);
                IceaxeIoUtil.getAndCloseFuture(lowShutdownFuture, //
                        timeout, IceaxeErrorCode.SESSION_SHUTDOWN_TIMEOUT, //
                        IceaxeErrorCode.SESSION_SHUTDOWN_CLOSE_TIMEOUT);
            });
        } catch (Throwable e) {
            LOG.trace("session shutdown error", e);
            occurred = e;
            throw e;
        } finally {
            var finalOccurred = occurred;
            event(occurred, listener -> listener.shutdownSession(this, shutdownType, timeoutNanos, finalOccurred));
        }
        LOG.trace("session shutdown end");
    }

    /**
     * shutdown.
     *
     * @param lowSession0     low session
     * @param lowShutdownType low shutdown type
     * @return future for shutdown
     * @throws IOException if I/O error was occurred while sending request
     */
    protected FutureResponse<Void> shutdownLow(Session lowSession0, ShutdownType lowShutdownType) throws IOException {
        return lowSession0.shutdown(lowShutdownType);
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void close() throws IOException, InterruptedException {
        close(closeTimeout.getNanos());
    }

    @Override
    public void close(long timeoutNanos) throws IOException, InterruptedException {
        this.closed = true;

        LOG.trace("session close start");
        Throwable occurred = null;
        try {
            IceaxeIoUtil.close(timeoutNanos, closeableSet, IceaxeErrorCode.SESSION_CHILD_CLOSE_ERROR, t -> {
                IceaxeTimeoutCloseable shutdownCloseable = shutdownTimeout -> {
                    if (this.lowSession != null) {
                        shutdown(getCloseShutdownType(), shutdownTimeout);
                    }
                };
                IceaxeIoUtil.close(t, IceaxeErrorCode.SESSION_CLOSE_TIMEOUT, IceaxeErrorCode.SESSION_CLOSE_ERROR, //
                        lowSqlClient, shutdownCloseable, lowSession, lowSessionFuture);
            });
        } catch (Throwable e) {
            LOG.trace("session close error", e);
            occurred = e;
            throw e;
        } finally {
            var finalOccurred = occurred;
            event(occurred, listener -> listener.closeSession(this, timeoutNanos, finalOccurred));
        }
        LOG.trace("session close end");
    }

    /**
     * Returns the closed state of the transaction.
     *
     * @return {@code true} if the transaction has been closed
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
            throw new IceaxeIOException(IceaxeErrorCode.SESSION_ALREADY_CLOSED);
        }
    }

    @Override
    public String toString() {
        String label = sessionOption.getLabel();
        if (label == null) {
            return getClass().getSimpleName() + "@" + Integer.toHexString(hashCode());
        }
        return getClass().getSimpleName() + "(" + label + ")";
    }
}
