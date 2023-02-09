package com.tsurugidb.iceaxe.session;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.explain.TsurugiExplainHelper;
import com.tsurugidb.iceaxe.metadata.TgTableMetadata;
import com.tsurugidb.iceaxe.metadata.TsurugiTableMetadataHelper;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.event.TsurugiSessionEventListener;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery0;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery1;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate0;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.util.IceaxeCloseableSet;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.TgTimeValue;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi Session
 */
public class TsurugiSession implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiSession.class);

    private final TgSessionInfo sessionInfo;
    private final Session lowSession;
    private FutureResponse<Wire> lowWireFuture;
    private Throwable lowFutureException = null;
    private boolean sessionConnected = false;
    private SqlClient lowSqlClient;
    private TsurugiTableMetadataHelper tableMetadataHelper = null;
    private TsurugiExplainHelper explainHelper = null;
    private IceaxeConvertUtil convertUtil = null;
    private final IceaxeTimeout connectTimeout;
    private final IceaxeTimeout closeTimeout;
    private List<TsurugiSessionEventListener> eventListenerList = null;
    private final IceaxeCloseableSet closeableSet = new IceaxeCloseableSet();
    private boolean closed = false;

    // internal
    public TsurugiSession(TgSessionInfo info, Session lowSession, FutureResponse<Wire> lowWireFuture) {
        this.sessionInfo = info;
        this.lowSession = lowSession;
        this.lowWireFuture = lowWireFuture;
        this.connectTimeout = new IceaxeTimeout(info, TgTimeoutKey.SESSION_CONNECT);
        this.closeTimeout = new IceaxeTimeout(info, TgTimeoutKey.SESSION_CLOSE);

        applyCloseTimeout();
    }

    private void applyCloseTimeout() {
        closeTimeout.apply(lowSession);
        closeTimeout.apply(lowWireFuture);
        closeTimeout.apply(lowSqlClient);
    }

    /**
     * set convert type utility
     *
     * @param convertUtil convert type utility
     */
    public void setConvertUtil(IceaxeConvertUtil convertUtil) {
        this.convertUtil = convertUtil;
    }

    // internal
    public IceaxeConvertUtil getConvertUtil() {
        return this.convertUtil;
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

    // internal
    public final TgSessionInfo getSessionInfo() {
        return sessionInfo;
    }

    /**
     * add event listener
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

    private void event(Throwable occurred, Consumer<TsurugiSessionEventListener> action) {
        if (this.eventListenerList != null) {
            try {
                for (var listener : eventListenerList) {
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

    // internal
//  @ThreadSafe
    public final synchronized SqlClient getLowSqlClient() throws IOException {
        if (this.lowSqlClient == null) {
            var lowSession = getLowSession();
            LOG.trace("SqlClient.attach start");
            this.lowSqlClient = SqlClient.attach(lowSession);
            LOG.trace("SqlClient.attach end");
            applyCloseTimeout();
        }
        return this.lowSqlClient;
    }

//  @ThreadSafe
    protected final synchronized Session getLowSession() throws IOException {
        if (!this.sessionConnected) {
            if (this.lowFutureException != null) {
                throw new TsurugiIOException(IceaxeErrorCode.SESSION_LOW_ERROR, lowFutureException);
            }

            LOG.trace("lowSession.wire get start");
            Wire lowWire;
            try {
                lowWire = IceaxeIoUtil.getAndCloseFuture(lowWireFuture, connectTimeout);
            } catch (Throwable e) {
                this.lowFutureException = e;
                throw e;
            }
            LOG.trace("lowSession.wire connect");
            lowSession.connect(lowWire);
            LOG.trace("lowSession.wire get end");
            this.sessionConnected = true;

            this.lowWireFuture = null;
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
            getLowSession();
        } catch (IOException e) {
            LOG.trace("exception in isAlive()", e);
            // fall through
        }
        return lowSession.isAlive();
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
     * get table metadata
     *
     * @param tableName table name
     * @return table metadata (empty if table not found)
     * @throws IOException
     */
//  @ThreadSafe
    public Optional<TgTableMetadata> findTableMetadata(String tableName) throws IOException {
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
     * create PreparedStatement
     *
     * @param sql SQL
     * @return PreparedStatement
     * @throws IOException
     */
//  @ThreadSafe
    public TsurugiPreparedStatementQuery0<TsurugiResultEntity> createPreparedQuery(String sql) throws IOException {
        return createPreparedQuery(sql, TgResultMapping.DEFAULT);
    }

    /**
     * create PreparedStatement
     *
     * @param <R>           result type
     * @param sql           SQL
     * @param resultMapping result mapping
     * @return PreparedStatement
     * @throws IOException
     */
//  @ThreadSafe
    public <R> TsurugiPreparedStatementQuery0<R> createPreparedQuery(String sql, TgResultMapping<R> resultMapping) throws IOException {
        checkClose();
        LOG.trace("createPreparedQuery. sql={}", sql);
        var ps = new TsurugiPreparedStatementQuery0<>(this, sql, resultMapping);
        event(null, listener -> listener.createQuery(ps));
        return ps;
    }

    /**
     * create PreparedStatement
     *
     * @param <P>              parameter type
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @return PreparedStatement
     * @throws IOException
     */
//  @ThreadSafe
    public <P> TsurugiPreparedStatementQuery1<P, TsurugiResultEntity> createPreparedQuery(String sql, TgParameterMapping<P> parameterMapping) throws IOException {
        return createPreparedQuery(sql, parameterMapping, TgResultMapping.DEFAULT);
    }

    /**
     * create PreparedStatement
     *
     * @param <P>              parameter type
     * @param <R>              result type
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @param resultMapping    result mapping
     * @return PreparedStatement
     * @throws IOException
     */
//  @ThreadSafe
    public <P, R> TsurugiPreparedStatementQuery1<P, R> createPreparedQuery(String sql, TgParameterMapping<P> parameterMapping, TgResultMapping<R> resultMapping) throws IOException {
        checkClose();
        LOG.trace("createPreparedQuery start. sql={}", sql);
        var lowPlaceholderList = parameterMapping.toLowPlaceholderList();
        var lowPreparedStatementFuture = getLowSqlClient().prepare(sql, lowPlaceholderList);
        LOG.trace("createPreparedQuery started");
        var ps = new TsurugiPreparedStatementQuery1<>(this, sql, lowPreparedStatementFuture, parameterMapping, resultMapping);
        event(null, listener -> listener.createQuery(ps));
        return ps;
    }

    /**
     * create PreparedStatement
     *
     * @param sql SQL
     * @return PreparedStatement
     * @throws IOException
     */
//  @ThreadSafe
    public TsurugiPreparedStatementUpdate0 createPreparedStatement(String sql) throws IOException {
        checkClose();
        LOG.trace("createPreparedStatement. sql={}", sql);
        var ps = new TsurugiPreparedStatementUpdate0(this, sql);
        event(null, listener -> listener.createStatement(ps));
        return ps;
    }

    /**
     * create PreparedStatement
     *
     * @param <P>              parameter type
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @return PreparedStatement
     * @throws IOException
     */
//  @ThreadSafe
    public <P> TsurugiPreparedStatementUpdate1<P> createPreparedStatement(String sql, TgParameterMapping<P> parameterMapping) throws IOException {
        checkClose();
        LOG.trace("createPreparedStatement start. sql={}", sql);
        var lowPlaceholderList = parameterMapping.toLowPlaceholderList();
        var lowPreparedStatementFuture = getLowSqlClient().prepare(sql, lowPlaceholderList);
        LOG.trace("createPreparedStatement started");
        var ps = new TsurugiPreparedStatementUpdate1<>(this, sql, lowPreparedStatementFuture, parameterMapping);
        event(null, listener -> listener.createStatement(ps));
        return ps;
    }

    /**
     * create transaction manager
     *
     * @return Transaction Manager
     */
//  @ThreadSafe
    public TsurugiTransactionManager createTransactionManager() {
        return createTransactionManager((TgTmSetting) null);
    }

    /**
     * create transaction manager
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
     * create transaction manager
     *
     * @param setting transaction manager settings
     * @return Transaction Manager
     */
//  @ThreadSafe
    public TsurugiTransactionManager createTransactionManager(TgTxOption option) {
        var setting = TgTmSetting.of(option);
        return createTransactionManager(setting);
    }

    /**
     * create Transaction
     *
     * @param option transaction option
     * @return Transaction
     * @throws IOException
     */
//  @ThreadSafe
    public TsurugiTransaction createTransaction(@Nonnull TgTxOption option) throws IOException {
        return createTransaction(option, null);
    }

    /**
     * create Transaction
     *
     * @param option      transaction option
     * @param initializer transaction initializer
     * @return Transaction
     * @throws IOException
     */
//  @ThreadSafe
    public TsurugiTransaction createTransaction(@Nonnull TgTxOption option, @Nullable Consumer<TsurugiTransaction> initializer) throws IOException {
        checkClose();

        var lowOption = option.toLowTransactionOption();
        LOG.trace("lowTransaction create start. lowOption={}", lowOption);
        var lowTransactionFuture = getLowSqlClient().createTransaction(lowOption);
        LOG.trace("lowTransaction create started");
        var transaction = new TsurugiTransaction(this, lowTransactionFuture, option);
        if (initializer != null) {
            initializer.accept(transaction);
        }
        event(null, listener -> listener.createTransaction(transaction));
        return transaction;
    }

    // internal
    public void addChild(Closeable closeable) throws IOException {
        checkClose();
        closeableSet.add(closeable);
    }

    // internal
    public void removeChild(Closeable closeable) {
        closeableSet.remove(closeable);
    }

    @Override
    public void close() throws IOException {
        this.closed = true;

        LOG.trace("session close start");
        Throwable occurred = null;
        try {
            IceaxeIoUtil.close(closeableSet, () -> {
                IceaxeIoUtil.close(lowSqlClient, lowSession, lowWireFuture);
            });
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalOccurred = occurred;
            event(occurred, listener -> listener.closeSession(this, finalOccurred));
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

    protected void checkClose() throws IOException {
        if (isClosed()) {
            throw new TsurugiIOException(IceaxeErrorCode.SESSION_ALREADY_CLOSED);
        }
    }
}
