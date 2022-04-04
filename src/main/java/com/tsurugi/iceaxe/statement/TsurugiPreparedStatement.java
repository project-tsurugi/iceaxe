package com.tsurugi.iceaxe.statement;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;

import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.tsurugi.iceaxe.session.TsurugiSession;
import com.tsurugi.iceaxe.util.IceaxeIoUtil;

/**
 * Tsurugi PreparedStatement
 * 
 * @param <P> parameter type
 * @param <R> result record type
 */
public class TsurugiPreparedStatement<P, R> implements Closeable {

    private final TsurugiSession ownerSession;
    private Future<PreparedStatement> lowPreparedStatementFuture;
    private PreparedStatement lowPreparedStatement;

    // internal
    public TsurugiPreparedStatement(TsurugiSession session, Future<PreparedStatement> lowPreparedStatementFuture) {
        this.ownerSession = session;
        this.lowPreparedStatementFuture = lowPreparedStatementFuture;
    }

    protected final PreparedStatement getLowPreparedStatement() throws IOException {
        if (this.lowPreparedStatementFuture != null) {
            var info = ownerSession.getSessionInfo();
            this.lowPreparedStatement = IceaxeIoUtil.getFromFuture(lowPreparedStatementFuture, info);
            lowPreparedStatement.setCloseTimeout(info.timeoutTime(), info.timeoutUnit());
            this.lowPreparedStatementFuture = null;
        }
        return this.lowPreparedStatement;
    }

    @Override
    public void close() throws IOException {
        getLowPreparedStatement().close();
        ownerSession.removeChild(this);
    }
}
