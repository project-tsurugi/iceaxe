package com.tsurugi.iceaxe.session;

import java.io.Closeable;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Future;
import java.util.function.Function;

import com.nautilus_technologies.tsubakuro.channel.common.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos.TransactionOption;
import com.tsurugi.iceaxe.result.TsurugiResultRecord;
import com.tsurugi.iceaxe.statement.TgParameter;
import com.tsurugi.iceaxe.statement.TgVariable;
import com.tsurugi.iceaxe.statement.TsurugiPreparedStatementQuery0;
import com.tsurugi.iceaxe.statement.TsurugiPreparedStatementQuery1;
import com.tsurugi.iceaxe.statement.TsurugiPreparedStatementUpdate0;
import com.tsurugi.iceaxe.statement.TsurugiPreparedStatementUpdate1;
import com.tsurugi.iceaxe.transaction.TsurugiTransaction;
import com.tsurugi.iceaxe.util.IceaxeIoUtil;

/**
 * Tsurugi Session
 */
public class TsurugiSession implements Closeable {

    private final TgSessionInfo sessionInfo;
    private final Session lowSession;
    private Future<SessionWire> lowSessionWireFuture;
    private final NavigableSet<Closeable> closeableSet = new ConcurrentSkipListSet<>();

    // internal
    public TsurugiSession(TgSessionInfo info, Session lowSession, Future<SessionWire> lowSessionWireFuture) {
        this.sessionInfo = info;
        this.lowSession = lowSession;
        this.lowSessionWireFuture = lowSessionWireFuture;

        lowSession.setCloseTimeout(info.timeoutTime(), info.timeoutUnit());
    }

    public final TgSessionInfo getSessionInfo() {
        return sessionInfo;
    }

    protected final synchronized Session getLowSession() throws IOException {
        if (this.lowSessionWireFuture != null) {
            var lowSessionWire = IceaxeIoUtil.getFromFuture(lowSessionWireFuture, sessionInfo);
            lowSession.connect(lowSessionWire);
            this.lowSessionWireFuture = null;
        }
        return lowSession;
    }

    public <R> TsurugiPreparedStatementQuery0<R> createPreparedQuery(String sql, Function<TsurugiResultRecord, R> recordConverter) throws IOException {
        var ps = new TsurugiPreparedStatementQuery0<>(this, sql, recordConverter);
        closeableSet.add(ps);
        return ps;
    }

    public <P, R> TsurugiPreparedStatementQuery1<P, R> createPreparedQuery(String sql, TgVariable variable, Function<P, TgParameter> parameterConverter,
            Function<TsurugiResultRecord, R> recordConverter) throws IOException {
        var lowPlaceHolder = variable.toLowPlaceHolder();
        var lowPreparedStatementFuture = getLowSession().prepare(sql, lowPlaceHolder);
        var ps = new TsurugiPreparedStatementQuery1<>(this, lowPreparedStatementFuture, parameterConverter, recordConverter);
        closeableSet.add(ps);
        return ps;
    }

    public TsurugiPreparedStatementUpdate0 createPreparedStatement(String sql) throws IOException {
        var ps = new TsurugiPreparedStatementUpdate0(this, sql);
        closeableSet.add(ps);
        return ps;
    }

    public <P> TsurugiPreparedStatementUpdate1<P> createPreparedStatement(String sql, TgVariable variable, Function<P, TgParameter> parameterConverter) throws IOException {
        var lowPlaceHolder = variable.toLowPlaceHolder();
        var lowPreparedStatementFuture = getLowSession().prepare(sql, lowPlaceHolder);
        var ps = new TsurugiPreparedStatementUpdate1<>(this, lowPreparedStatementFuture, parameterConverter);
        closeableSet.add(ps);
        return ps;
    }

    public TsurugiTransaction createTransaction() throws IOException { // TODO TransactionOption
        var lowOption = TransactionOption.newBuilder().build();
        var lowTransactionFuture = getLowSession().createTransaction(lowOption);
        var transaction = new TsurugiTransaction(this, lowTransactionFuture);
        closeableSet.add(transaction);
        return transaction;
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
