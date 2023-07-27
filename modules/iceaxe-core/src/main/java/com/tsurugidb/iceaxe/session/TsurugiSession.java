package com.tsurugidb.iceaxe.session;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
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
import com.tsurugidb.iceaxe.util.IceaxeCloseableSet;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.TgTimeValue;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi Session
 */
public class TsurugiSession implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiSession.class);

    private final TgSessionOption sessionOption;
    private FutureResponse<? extends Session> lowSessionFuture;
    private Session lowSession;
    private Throwable lowFutureException = null;
    private SqlClient lowSqlClient;
    private TsurugiTableListHelper tableListHelper = null;
    private TsurugiTableMetadataHelper tableMetadataHelper = null;
    private TsurugiExplainHelper explainHelper = null;
    private IceaxeConvertUtil convertUtil = null;
    private final IceaxeTimeout connectTimeout;
    private final IceaxeTimeout closeTimeout;
    private List<TsurugiSessionEventListener> eventListenerList = null;
    private final IceaxeCloseableSet closeableSet = new IceaxeCloseableSet();
    private boolean closed = false;

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

        applyCloseTimeout();
    }

    private void applyCloseTimeout() {
        closeTimeout.apply(lowSessionFuture);
        closeTimeout.apply(lowSession);
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

    /**
     * get convert type utility
     *
     * @return convert type utility
     */
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

    /**
     * get session option
     *
     * @return session option
     */
    public final @Nonnull TgSessionOption getSessionOption() {
        return this.sessionOption;
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

    /**
     * get SQL service client
     *
     * @return SQL service client
     * @throws IOException
     * @throws InterruptedException
     */
    @IceaxeInternal
//  @ThreadSafe
    public final synchronized SqlClient getLowSqlClient() throws IOException, InterruptedException {
        if (this.lowSqlClient == null) {
            var lowSession = getLowSession();
            LOG.trace("SqlClient.attach start");
            this.lowSqlClient = SqlClient.attach(lowSession);
            LOG.trace("SqlClient.attach end");
            applyCloseTimeout();
        }
        return this.lowSqlClient;
    }

    /**
     * get session
     *
     * @return session
     * @throws IOException
     * @throws InterruptedException
     */
    @IceaxeInternal
//  @ThreadSafe
    public final synchronized Session getLowSession() throws IOException, InterruptedException {
        if (this.lowSession == null) {
            if (this.lowFutureException != null) {
                throw new TsurugiIOException(IceaxeErrorCode.SESSION_LOW_ERROR, lowFutureException);
            }

            LOG.trace("lowSession get start");
            try {
                this.lowSession = IceaxeIoUtil.getAndCloseFuture(lowSessionFuture, connectTimeout);
            } catch (Throwable e) {
                this.lowFutureException = e;
                throw e;
            }
            LOG.trace("lowSession get end");

            this.lowSessionFuture = null;
            applyCloseTimeout();
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
     * @throws IOException
     * @throws InterruptedException
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
     * get table metadata
     *
     * @param tableName table name
     * @return table metadata (empty if table not found)
     * @throws IOException
     * @throws InterruptedException
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
     * create SQL query
     *
     * @param sql SQL
     * @return SQL query
     * @throws IOException
     */
//  @ThreadSafe
    public TsurugiSqlQuery<TsurugiResultEntity> createQuery(String sql) throws IOException {
        return createQuery(sql, TgResultMapping.DEFAULT);
    }

    /**
     * create SQL query
     *
     * @param <R>           result type
     * @param sql           SQL
     * @param resultMapping result mapping
     * @return SQL query
     * @throws IOException
     */
//  @ThreadSafe
    public <R> TsurugiSqlQuery<R> createQuery(String sql, TgResultMapping<R> resultMapping) throws IOException {
        checkClose();
        LOG.trace("createQuery. sql={}", sql);
        var ps = new TsurugiSqlQuery<>(this, sql, resultMapping);
        event(null, listener -> listener.createQuery(ps));
        return ps;
    }

    /**
     * create SQL prepared query
     *
     * @param <P>              parameter type
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @return SQL prepared query
     * @throws IOException
     * @throws InterruptedException
     */
//  @ThreadSafe
    public <P> TsurugiSqlPreparedQuery<P, TsurugiResultEntity> createQuery(String sql, TgParameterMapping<P> parameterMapping) throws IOException, InterruptedException {
        return createQuery(sql, parameterMapping, TgResultMapping.DEFAULT);
    }

    /**
     * create SQL prepared query
     *
     * @param <P>              parameter type
     * @param <R>              result type
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @param resultMapping    result mapping
     * @return SQL prepared query
     * @throws IOException
     * @throws InterruptedException
     */
//  @ThreadSafe
    public <P, R> TsurugiSqlPreparedQuery<P, R> createQuery(String sql, TgParameterMapping<P> parameterMapping, TgResultMapping<R> resultMapping) throws IOException, InterruptedException {
        checkClose();
        LOG.trace("createQuery start. sql={}", sql);
        var lowPlaceholderList = parameterMapping.toLowPlaceholderList();
        var lowPreparedStatementFuture = getLowSqlClient().prepare(sql, lowPlaceholderList);
        LOG.trace("createQuery started");
        var ps = new TsurugiSqlPreparedQuery<>(this, sql, lowPreparedStatementFuture, parameterMapping, resultMapping);
        event(null, listener -> listener.createQuery(ps));
        return ps;
    }

    /**
     * create SQL definition
     *
     * @param sql SQL
     * @return SQL definition
     * @throws IOException
     */
//  @ThreadSafe
    public TsurugiSqlStatement createStatement(String sql) throws IOException {
        checkClose();
        LOG.trace("createStatement. sql={}", sql);
        var ps = new TsurugiSqlStatement(this, sql);
        event(null, listener -> listener.createStatement(ps));
        return ps;
    }

    /**
     * create SQL prepared statement
     *
     * @param <P>              parameter type
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @return SQL prepared statement
     * @throws IOException
     * @throws InterruptedException
     */
//  @ThreadSafe
    public <P> TsurugiSqlPreparedStatement<P> createStatement(String sql, TgParameterMapping<P> parameterMapping) throws IOException, InterruptedException {
        checkClose();
        LOG.trace("createStatement start. sql={}", sql);
        var lowPlaceholderList = parameterMapping.toLowPlaceholderList();
        var lowPreparedStatementFuture = getLowSqlClient().prepare(sql, lowPlaceholderList);
        LOG.trace("createStatement started");
        var ps = new TsurugiSqlPreparedStatement<>(this, sql, lowPreparedStatementFuture, parameterMapping);
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
     * @param txOption transaction option
     * @return Transaction Manager
     */
//  @ThreadSafe
    public TsurugiTransactionManager createTransactionManager(TgTxOption txOption) {
        var setting = TgTmSetting.of(txOption);
        return createTransactionManager(setting);
    }

    /**
     * create Transaction
     *
     * @param txOption transaction option
     * @return Transaction
     * @throws IOException
     * @throws InterruptedException
     */
//  @ThreadSafe
    public TsurugiTransaction createTransaction(@Nonnull TgTxOption txOption) throws IOException, InterruptedException {
        return createTransaction(txOption, null);
    }

    /**
     * create Transaction
     *
     * @param txOption    transaction option
     * @param initializer transaction initializer
     * @return Transaction
     * @throws IOException
     * @throws InterruptedException
     */
//  @ThreadSafe
    public TsurugiTransaction createTransaction(@Nonnull TgTxOption txOption, @Nullable Consumer<TsurugiTransaction> initializer) throws IOException, InterruptedException {
        checkClose();

        var lowOption = txOption.toLowTransactionOption();
        LOG.trace("lowTransaction create start. lowOption={}", lowOption);
        var lowTransactionFuture = getLowSqlClient().createTransaction(lowOption);
        LOG.trace("lowTransaction create started");
        var transaction = new TsurugiTransaction(this, lowTransactionFuture, txOption);
        if (initializer != null) {
            initializer.accept(transaction);
        }
        event(null, listener -> listener.createTransaction(transaction));
        return transaction;
    }

    /**
     * add child object
     *
     * @param closeable child object
     * @throws IOException
     */
    @IceaxeInternal
    public void addChild(AutoCloseable closeable) throws IOException {
        checkClose();
        closeableSet.add(closeable);
    }

    /**
     * remove child object
     *
     * @param closeable child object
     */
    @IceaxeInternal
    public void removeChild(AutoCloseable closeable) {
        closeableSet.remove(closeable);
    }

    @Override
    public void close() throws IOException, InterruptedException {
        this.closed = true;

        LOG.trace("session close start");
        Throwable occurred = null;
        try {
            IceaxeIoUtil.close(closeableSet, () -> {
                IceaxeIoUtil.close(lowSqlClient, lowSession, lowSessionFuture);
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

    @Override
    public String toString() {
        String label = sessionOption.getLabel();
        if (label == null) {
            return getClass().getSimpleName() + "@" + Integer.toHexString(hashCode());
        }
        return getClass().getSimpleName() + "(" + label + ")";
    }
}
