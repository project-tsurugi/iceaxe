package com.tsurugi.iceaxe.statement;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.function.Function;

import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.tsurugi.iceaxe.result.TsurugiResultCount;
import com.tsurugi.iceaxe.session.TsurugiSession;
import com.tsurugi.iceaxe.transaction.TsurugiTransaction;
import com.tsurugi.iceaxe.transaction.TsurugiTransactionIOException;

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
    public TsurugiPreparedStatementUpdate1(TsurugiSession session, Future<PreparedStatement> lowPreparedStatementFuture, Function<P, TgParameterList> parameterConverter) {
        super(session, lowPreparedStatementFuture, parameterConverter);
    }

    /**
     * execute statement
     * 
     * @param transaction Transaction
     * @param parameter   SQL parameter
     * @return result
     * @throws TsurugiTransactionIOException
     */
    public TsurugiResultCount execute(TsurugiTransaction transaction, P parameter) throws TsurugiTransactionIOException {
        try {
            var lowTransaction = transaction.getLowTransaction();
            var lowPs = getLowPreparedStatement();
            var lowParameterSet = getLowParameterSet(parameter);
            var lowResultFuture = lowTransaction.executeStatement(lowPs, lowParameterSet);
            var result = new TsurugiResultCount(transaction, lowResultFuture);
            return result;
        } catch (IOException e) {
            throw new TsurugiTransactionIOException(e);
        }
    }
}
