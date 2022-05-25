package com.tsurugi.iceaxe.session;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.nautilus_technologies.tsubakuro.channel.common.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.tsurugi.iceaxe.result.TgResultMapping;
import com.tsurugi.iceaxe.result.TsurugiResultEntity;
import com.tsurugi.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugi.iceaxe.statement.TgParameterMapping;
import com.tsurugi.iceaxe.statement.TsurugiPreparedStatementQuery0;
import com.tsurugi.iceaxe.statement.TsurugiPreparedStatementQuery1;
import com.tsurugi.iceaxe.statement.TsurugiPreparedStatementUpdate0;
import com.tsurugi.iceaxe.statement.TsurugiPreparedStatementUpdate1;
import com.tsurugi.iceaxe.transaction.TgTransactionOption;
import com.tsurugi.iceaxe.transaction.TsurugiTransaction;
import com.tsurugi.iceaxe.transaction.TsurugiTransactionManager;
import com.tsurugi.iceaxe.util.IceaxeIoUtil;
import com.tsurugi.iceaxe.util.TgTimeValue;

/**
 * Tsurugi Session
 */
public class TsurugiSession implements Closeable {

    private final TgSessionInfo sessionInfo;
    private final Session lowSession;
    private Future<SessionWire> lowSessionWireFuture;
    private TgTimeValue connectTimeout;
    private final NavigableSet<Closeable> closeableSet = new ConcurrentSkipListSet<>();

    // internal
    public TsurugiSession(TgSessionInfo info, Session lowSession, Future<SessionWire> lowSessionWireFuture) {
        this.sessionInfo = info;
        this.lowSession = lowSession;
        this.lowSessionWireFuture = lowSessionWireFuture;
        setConnectTimeout(info.timeout(TgTimeoutKey.SESSION_CONNECT));
        setCloseTimeout(info.timeout(TgTimeoutKey.SESSION_CLOSE));
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
        lowSession.setCloseTimeout(timeout.value(), timeout.unit());
    }

    // internal
    public final TgSessionInfo getSessionInfo() {
        return sessionInfo;
    }

    protected final synchronized Session getLowSession() throws IOException {
        if (this.lowSessionWireFuture != null) {
            var lowSessionWire = IceaxeIoUtil.getFromFuture(lowSessionWireFuture, connectTimeout);
            lowSession.connect(lowSessionWire);
            this.lowSessionWireFuture = null;
        }
        return lowSession;
    }

    /**
     * create PreparedStatement
     * 
     * @param sql SQL
     * @return PreparedStatement
     * @throws IOException
     */
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
    public <R> TsurugiPreparedStatementQuery0<R> createPreparedQuery(String sql, TgResultMapping<R> resultMapping) throws IOException {
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
    public <P, R> TsurugiPreparedStatementQuery1<P, R> createPreparedQuery(String sql, TgParameterMapping<P> parameterMapping, TgResultMapping<R> resultMapping) throws IOException {
        var lowPlaceHolder = parameterMapping.toLowPlaceHolder();
        var lowPreparedStatementFuture = getLowSession().prepare(sql, lowPlaceHolder);
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
    public TsurugiPreparedStatementUpdate0 createPreparedStatement(String sql) throws IOException {
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
    public <P> TsurugiPreparedStatementUpdate1<P> createPreparedStatement(String sql, TgParameterMapping<P> parameterMapping) throws IOException {
        var lowPlaceHolder = parameterMapping.toLowPlaceHolder();
        var lowPreparedStatementFuture = getLowSession().prepare(sql, lowPlaceHolder);
        var ps = new TsurugiPreparedStatementUpdate1<>(this, lowPreparedStatementFuture, parameterMapping);
        return ps;
    }

    /**
     * create transaction manager
     * 
     * @return Transaction Manager
     */
    public TsurugiTransactionManager createTransactionManager() {
        return createTransactionManager(null);
    }

    /**
     * create transaction manager
     * 
     * @param defaultTransactionOptionList transaction option
     * 
     * @return Transaction Manager
     */
    public TsurugiTransactionManager createTransactionManager(List<TgTransactionOption> defaultTransactionOptionList) {
        var tm = new TsurugiTransactionManager(this, defaultTransactionOptionList);
        return tm;
    }

    /**
     * create Transaction
     * 
     * @param option transaction option
     * @return Transaction
     * @throws IOException
     */
    public TsurugiTransaction createTransaction(TgTransactionOption option) throws IOException {
        var lowOption = option.toLowTransactionOption();
        var lowTransactionFuture = getLowSession().createTransaction(lowOption);
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
        IceaxeIoUtil.close(closeableSet, () -> {
            try {
                getLowSession();
            } finally {
                lowSession.close();
            }
        });
    }
}
