package com.tsurugi.iceaxe.statement;

import java.io.IOException;
import java.util.concurrent.Future;

import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.tsurugi.iceaxe.session.TsurugiSession;
import com.tsurugi.iceaxe.util.IceaxeIoUtil;

/**
 * Tsurugi PreparedStatement with Low-PreparedStatement
 */
public abstract class TsurugiPreparedStatementWithLowPs extends TsurugiPreparedStatement {

    private Future<PreparedStatement> lowPreparedStatementFuture;
    private PreparedStatement lowPreparedStatement;

    protected TsurugiPreparedStatementWithLowPs(TsurugiSession session, Future<PreparedStatement> lowPreparedStatementFuture) {
        super(session);
        this.lowPreparedStatementFuture = lowPreparedStatementFuture;
    }

    protected synchronized final PreparedStatement getLowPreparedStatement() throws IOException {
        if (this.lowPreparedStatement == null) {
            var info = getSessionInfo();
            this.lowPreparedStatement = IceaxeIoUtil.getFromFuture(lowPreparedStatementFuture, info);
            lowPreparedStatement.setCloseTimeout(info.timeoutTime(), info.timeoutUnit());
            this.lowPreparedStatementFuture = null;
        }
        return this.lowPreparedStatement;
    }

    @Override
    public void close() throws IOException {
        // not try-finally
        getLowPreparedStatement().close();
        super.close();
    }
}
