package com.tsurugidb.iceaxe.session;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.channel.common.connection.wire.Wire;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery0;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery1;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate0;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.util.IceaxeCloseableSet;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.TgTimeValue;

/**
 * Tsurugi Session
 */
public class TsurugiSession implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiSession.class);

    private final TgSessionInfo sessionInfo;
    private final Session lowSession;
    private FutureResponse<Wire> lowWireFuture;
    private boolean sessionConnected = false;
    private SqlClient lowSqlClient;
    private IceaxeConvertUtil convertUtil = null;
    private final IceaxeTimeout connectTimeout;
    private final IceaxeTimeout closeTimeout;
    private final IceaxeCloseableSet closeableSet = new IceaxeCloseableSet();

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

//  @ThreadSafe
    protected final synchronized SqlClient getLowSqlClient() throws IOException {
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
            LOG.trace("lowSession.wire get start");
            var lowWire = IceaxeIoUtil.getFromFuture(lowWireFuture, connectTimeout);
            LOG.trace("lowSession.wire connect");
            lowSession.connect(lowWire);
            LOG.trace("lowSession.wire get end");
            this.sessionConnected = true;

            IceaxeIoUtil.close(lowWireFuture);
            this.lowWireFuture = null;
        }
        return this.lowSession;
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
        LOG.trace("createPreparedQuery. sql={}", sql);
        var ps = new TsurugiPreparedStatementQuery0<>(this, sql, resultMapping);
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
        LOG.trace("createPreparedQuery start. sql={}", sql);
        var lowPlaceholderList = parameterMapping.toLowPlaceholderList();
        var lowPreparedStatementFuture = getLowSqlClient().prepare(sql, lowPlaceholderList);
        LOG.trace("createPreparedQuery started");
        var ps = new TsurugiPreparedStatementQuery1<>(this, lowPreparedStatementFuture, parameterMapping, resultMapping);
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
        LOG.trace("createPreparedStatement. sql={}", sql);
        var ps = new TsurugiPreparedStatementUpdate0(this, sql);
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
        LOG.trace("createPreparedStatement start. sql={}", sql);
        var lowPlaceholderList = parameterMapping.toLowPlaceholderList();
        var lowPreparedStatementFuture = getLowSqlClient().prepare(sql, lowPlaceholderList);
        LOG.trace("createPreparedStatement started");
        var ps = new TsurugiPreparedStatementUpdate1<>(this, lowPreparedStatementFuture, parameterMapping);
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
        var setting = TgTmSetting.ofAlways(option, 1);
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
        var lowOption = option.toLowTransactionOption();
        LOG.trace("lowTransaction create start. lowOption={}", lowOption);
        var lowTransactionFuture = getLowSqlClient().createTransaction(lowOption);
        LOG.trace("lowTransaction create started");
        var transaction = new TsurugiTransaction(this, lowTransactionFuture);
        return transaction;
    }

    // internal
    public void addChild(Closeable closeable) {
        closeableSet.add(closeable);
    }

    // internal
    public void removeChild(Closeable closeable) {
        closeableSet.remove(closeable);
    }

    @Override
    public void close() throws IOException {
        LOG.trace("session close start");
        IceaxeIoUtil.close(closeableSet, () -> {
            IceaxeIoUtil.close(lowSqlClient, lowSession, lowWireFuture);
        });
        LOG.trace("session close end");
    }
}
