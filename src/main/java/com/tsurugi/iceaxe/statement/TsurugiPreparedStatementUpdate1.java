package com.tsurugi.iceaxe.statement;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Future;

import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.tsurugi.iceaxe.result.TsurugiResultCount;
import com.tsurugi.iceaxe.session.TsurugiSession;
import com.tsurugi.iceaxe.transaction.TgTransactionOption;
import com.tsurugi.iceaxe.transaction.TsurugiTransaction;
import com.tsurugi.iceaxe.transaction.TsurugiTransactionIOException;
import com.tsurugi.iceaxe.transaction.TsurugiTransactionManager;

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
    public TsurugiPreparedStatementUpdate1(TsurugiSession session, Future<PreparedStatement> lowPreparedStatementFuture, TgParameterMapping<P> parameterMapping) {
        super(session, lowPreparedStatementFuture, parameterMapping);
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

    /**
     * execute statement
     * 
     * @param transaction Transaction
     * @param parameter   SQL parameter
     * @return row count
     * @throws TsurugiTransactionIOException
     */
    public int executeAndGetCount(TsurugiTransaction transaction, P parameter) throws TsurugiTransactionIOException {
        try (var result = execute(transaction, parameter)) {
            return result.getUpdateCount();
        } catch (IOException e) {
            throw new TsurugiTransactionIOException(e);
        }
    }

    /**
     * execute statement
     * 
     * @param tm        Transaction Manager
     * @param parameter SQL parameter
     * @return row count
     * @throws IOException
     */
    public int executeAndGetCount(TsurugiTransactionManager tm, P parameter) throws IOException {
        return tm.execute(transaction -> {
            return executeAndGetCount(transaction, parameter);
        });
    }

    /**
     * execute statement
     * 
     * @param tm                    Transaction Manager
     * @param transactionOptionList transaction option
     * @param parameter             SQL parameter
     * @return row count
     * @throws IOException
     */
    public int executeAndGetCount(TsurugiTransactionManager tm, List<TgTransactionOption> transactionOptionList, P parameter) throws IOException {
        return tm.execute(transactionOptionList, transaction -> {
            return executeAndGetCount(transaction, parameter);
        });
    }
}
