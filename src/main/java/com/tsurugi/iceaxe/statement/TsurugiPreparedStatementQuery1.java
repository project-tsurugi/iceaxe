package com.tsurugi.iceaxe.statement;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.function.Function;

import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.tsurugi.iceaxe.result.TsurugiResultRecord;
import com.tsurugi.iceaxe.result.TsurugiResultSet;
import com.tsurugi.iceaxe.session.TsurugiSession;
import com.tsurugi.iceaxe.transaction.TsurugiTransaction;

/**
 * Tsurugi PreparedStatement
 * <ul>
 * <li>TODO+++翻訳: クエリー系SQL</li>
 * <li>TODO+++翻訳: パラメーターあり</li>
 * </ul>
 * 
 * @param <P> parameter type
 * @param <R> record type
 */
public class TsurugiPreparedStatementQuery1<P, R> extends TsurugiPreparedStatementWithLowPs {

    private final Function<P, TgParameter> parameterConverter;
    private final Function<TsurugiResultRecord, R> recordConverter;

    // internal
    public TsurugiPreparedStatementQuery1(TsurugiSession session, Future<PreparedStatement> lowPreparedStatementFuture, Function<P, TgParameter> parameterConverter,
            Function<TsurugiResultRecord, R> recordConverter) {
        super(session, lowPreparedStatementFuture);
        this.parameterConverter = parameterConverter;
        this.recordConverter = recordConverter;
    }

    public TsurugiResultSet<R> execute(TsurugiTransaction transaction, P parameter) throws IOException {
        var lowTransaction = transaction.getLowTransaction();
        var lowPs = getLowPreparedStatement();
        var param = parameterConverter.apply(parameter);
        var lowParameterSet = param.toLowParameterSet();
        var lowResultPair = lowTransaction.executeQuery(lowPs, lowParameterSet);
        var result = new TsurugiResultSet<>(this, lowResultPair.getLeft(), lowResultPair.getRight(), recordConverter);
        addChild(result);
        return result;
    }
}
