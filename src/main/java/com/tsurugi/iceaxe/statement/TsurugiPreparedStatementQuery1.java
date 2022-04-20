package com.tsurugi.iceaxe.statement;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.function.Function;

import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.tsurugi.iceaxe.result.TsurugiResultRecord;
import com.tsurugi.iceaxe.result.TsurugiResultSet;
import com.tsurugi.iceaxe.session.TsurugiSession;
import com.tsurugi.iceaxe.transaction.TsurugiTransaction;
import com.tsurugi.iceaxe.transaction.TsurugiTransactionIOException;
import com.tsurugi.iceaxe.util.IoFunction;

/**
 * Tsurugi PreparedStatement
 * <ul>
 * <li>TODO+++翻訳: クエリー系SQL</li>
 * <li>TODO+++翻訳: SQLのパラメーターあり</li>
 * </ul>
 * 
 * @param <P> parameter type
 * @param <R> record type
 */
public class TsurugiPreparedStatementQuery1<P, R> extends TsurugiPreparedStatementWithLowPs<P> {

    private final IoFunction<TsurugiResultRecord, R> recordConverter;

    // internal
    public TsurugiPreparedStatementQuery1(TsurugiSession session, Future<PreparedStatement> lowPreparedStatementFuture, Function<P, TgParameter> parameterConverter,
            IoFunction<TsurugiResultRecord, R> recordConverter) {
        super(session, lowPreparedStatementFuture, parameterConverter);
        this.recordConverter = recordConverter;
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
            var result = new TsurugiResultSet<>(transaction, lowResultSetFuture, recordConverter);
            return result;
        } catch (IOException e) {
            throw new TsurugiTransactionIOException(e);
        }
    }
}
