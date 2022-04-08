package com.tsurugi.iceaxe.statement;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.function.Function;

import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.tsurugi.iceaxe.result.TsurugiResult;
import com.tsurugi.iceaxe.session.TsurugiSession;
import com.tsurugi.iceaxe.transaction.TsurugiTransaction;

/**
 * Tsurugi PreparedStatement
 * <ul>
 * <li>TODO+++翻訳: 更新系SQL</li>
 * <li>TODO+++翻訳: SQLのパラメーターあり</li>
 * </ul>
 * 
 * @param <P> parameter type
 */
public class TsurugiPreparedStatementUpdate1<P> extends TsurugiPreparedStatementWithLowPs<P> {

    // internal
    public TsurugiPreparedStatementUpdate1(TsurugiSession session, Future<PreparedStatement> lowPreparedStatementFuture, Function<P, TgParameter> parameterConverter) {
        super(session, lowPreparedStatementFuture, parameterConverter);
    }

    public TsurugiResult execute(TsurugiTransaction transaction, P parameter) throws IOException {
        var lowTransaction = transaction.getLowTransaction();
        var lowPs = getLowPreparedStatement();
        var lowParameterSet = getLowParameterSet(parameter);
        var lowResultFuture = lowTransaction.executeStatement(lowPs, lowParameterSet);
        var result = new TsurugiResult(this, lowResultFuture);
        addChild(result);
        return result;
    }
}
