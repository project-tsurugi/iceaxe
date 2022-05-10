package com.tsurugi.iceaxe.statement;

import java.io.IOException;
import java.util.concurrent.Future;

import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.tsurugi.iceaxe.result.TgResultMapping;
import com.tsurugi.iceaxe.result.TsurugiResultSet;
import com.tsurugi.iceaxe.session.TsurugiSession;
import com.tsurugi.iceaxe.transaction.TsurugiTransaction;
import com.tsurugi.iceaxe.transaction.TsurugiTransactionIOException;

/**
 * Tsurugi PreparedStatement
 * <ul>
 * <li>TODO+++翻訳: クエリー系SQL</li>
 * <li>TODO+++翻訳: SQLのパラメーターあり</li>
 * </ul>
 * 
 * @param <P> parameter type
 * @param <R> result type
 */
public class TsurugiPreparedStatementQuery1<P, R> extends TsurugiPreparedStatementWithLowPs<P> {

    private final TgResultMapping<R> resultMapping;

    // internal
    public TsurugiPreparedStatementQuery1(TsurugiSession session, Future<PreparedStatement> lowPreparedStatementFuture, TgParameterMapping<P> parameterMapping, TgResultMapping<R> resultMapping) {
        super(session, lowPreparedStatementFuture, parameterMapping);
        this.resultMapping = resultMapping;
    }

    /**
     * execute query
     * 
     * @param transaction Transaction
     * @param parameter   SQL parameter
     * @return result
     * @throws TsurugiTransactionIOException
     */
    public TsurugiResultSet<R> execute(TsurugiTransaction transaction, P parameter) throws TsurugiTransactionIOException {
        try {
            var lowTransaction = transaction.getLowTransaction();
            var lowPs = getLowPreparedStatement();
            var lowParameterSet = getLowParameterSet(parameter);
            var lowResultSetFuture = lowTransaction.executeQuery(lowPs, lowParameterSet);
            var result = new TsurugiResultSet<>(transaction, lowResultSetFuture, resultMapping);
            return result;
        } catch (IOException e) {
            throw new TsurugiTransactionIOException(e);
        }
    }
}
