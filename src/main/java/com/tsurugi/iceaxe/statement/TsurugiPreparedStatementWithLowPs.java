package com.tsurugi.iceaxe.statement;

import java.io.IOException;
import java.util.concurrent.Future;

import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos.ParameterSet;
import com.tsurugi.iceaxe.session.TsurugiSession;
import com.tsurugi.iceaxe.util.IceaxeIoUtil;

/**
 * Tsurugi PreparedStatement with Low-PreparedStatement
 * 
 * @param <P> parameter type
 */
public abstract class TsurugiPreparedStatementWithLowPs<P> extends TsurugiPreparedStatement {

    private Future<PreparedStatement> lowPreparedStatementFuture;
    private PreparedStatement lowPreparedStatement;
    private final TgParameterMapping<P> parameterMapping;

    protected TsurugiPreparedStatementWithLowPs(TsurugiSession session, Future<PreparedStatement> lowPreparedStatementFuture, TgParameterMapping<P> parameterMapping) {
        super(session);
        this.lowPreparedStatementFuture = lowPreparedStatementFuture;
        this.parameterMapping = parameterMapping;
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

    protected final ParameterSet getLowParameterSet(P parameter) {
        return parameterMapping.toLowParameterSet(parameter);
    }

    @Override
    public void close() throws IOException {
        // not try-finally
        getLowPreparedStatement().close();
        super.close();
    }
}
